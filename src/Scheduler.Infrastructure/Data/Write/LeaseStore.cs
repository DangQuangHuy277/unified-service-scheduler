using Npgsql;

namespace Scheduler.Infrastructure.Data;

/// <summary>Raised when a batch finds that a newer owner has taken the partition (D-007).</summary>
public sealed class PartitionLostException(int partition, long ourEpoch, long currentEpoch)
    : Exception($"Partition {partition} is now at epoch {currentEpoch}; this worker holds {ourEpoch}.");

/// <summary>Fencing tokens in <c>partition_lease</c>. Taking a lease waits for any in-flight batch of the previous owner to finish.</summary>
public sealed class LeaseStore(NpgsqlDataSource db)
{
    /// <summary>Called on partition assignment: the new owner's epoch is one higher than any earlier owner's.</summary>
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
    public async Task CheckAsync(NpgsqlConnection conn, NpgsqlTransaction tx, string topic, int partition, long epoch,
        CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(
            "SELECT epoch FROM partition_lease WHERE topic = $1 AND partition = $2 FOR UPDATE", conn, tx);
        cmd.Parameters.Add(new() { Value = topic });
        cmd.Parameters.Add(new() { Value = partition });
        var current = (long?)await cmd.ExecuteScalarAsync(ct) ?? 0;
        if (current != epoch) throw new PartitionLostException(partition, epoch, current);
    }
}
