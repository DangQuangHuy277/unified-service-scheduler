using System.Collections.Concurrent;
using Npgsql;
using Scheduler.Domain;

namespace Scheduler.Worker.Booking;

/// <summary>What this worker holds for one partition it owns: its fencing epoch and its dealerships' schedules.</summary>
public sealed class PartitionState(int partition, long epoch)
{
    public int Partition { get; } = partition;
    public long Epoch { get; } = epoch;
    public Dictionary<int, DealershipSchedule> Schedules { get; } = new();
}

/// <summary>Partitions this process currently owns. Read by tests and health output; written by the consumer thread.</summary>
public sealed class PartitionOwnership
{
    private readonly ConcurrentDictionary<int, PartitionState> _owned = new();

    public IReadOnlyCollection<int> OwnedPartitions => _owned.Keys.ToArray();
    public bool TryGet(int partition, out PartitionState state) => _owned.TryGetValue(partition, out state!);
    public void Set(PartitionState state) => _owned[state.Partition] = state;
    public void Remove(int partition) => _owned.TryRemove(partition, out _);
}

/// <summary>Raised when a batch finds that a newer owner has taken the partition (D-007).</summary>
public sealed class PartitionLostException(int partition, long ourEpoch, long currentEpoch)
    : Exception($"Partition {partition} is now at epoch {currentEpoch}; this worker holds {ourEpoch}.");

/// <summary>Fencing tokens. Taking a lease waits for any in-flight batch of the previous owner to finish.</summary>
public sealed class PartitionLeaseStore(NpgsqlDataSource db)
{
    public async Task<long> AcquireAsync(string topic, int partition, string owner, CancellationToken ct)
    {
        await using var cmd = db.CreateCommand("""
            INSERT INTO partition_lease (topic, partition, epoch, owner, acquired_at)
            VALUES ($1, $2, 1, $3, now())
            ON CONFLICT (topic, partition)
            DO UPDATE SET epoch = partition_lease.epoch + 1, owner = EXCLUDED.owner, acquired_at = now()
            RETURNING epoch
            """);
        cmd.Parameters.Add(new() { Value = topic });
        cmd.Parameters.Add(new() { Value = partition });
        cmd.Parameters.Add(new() { Value = owner });
        return (long)(await cmd.ExecuteScalarAsync(ct))!;
    }

    /// <summary>
    /// First statement of every batch transaction. The row lock makes a takeover (which updates the row)
    /// wait until this batch commits, and makes this batch fail if a takeover already happened.
    /// </summary>
    public static async Task CheckAsync(NpgsqlConnection conn, NpgsqlTransaction tx, string topic, PartitionState state, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(
            "SELECT epoch FROM partition_lease WHERE topic = $1 AND partition = $2 FOR UPDATE", conn, tx);
        cmd.Parameters.Add(new() { Value = topic });
        cmd.Parameters.Add(new() { Value = state.Partition });
        var current = (long?)await cmd.ExecuteScalarAsync(ct) ?? 0;
        if (current != state.Epoch) throw new PartitionLostException(state.Partition, state.Epoch, current);
    }
}
