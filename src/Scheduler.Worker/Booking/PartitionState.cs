using System.Collections.Concurrent;
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
