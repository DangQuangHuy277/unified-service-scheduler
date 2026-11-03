using Microsoft.Extensions.Options;
using Scheduler.Domain;
using Scheduler.Infrastructure.Data;

namespace Scheduler.Infrastructure;

public sealed record ReferenceSnapshot(
    IReadOnlyDictionary<int, DealershipProfile> Dealerships,
    IReadOnlyDictionary<string, ServiceType> ServiceTypes,
    DateTime LoadedAt);

/// <summary>
/// Dealerships, their technicians, bays and hours, and service types: small, read-mostly data that both the
/// API (to validate before enqueuing) and the workers (to decide) keep in memory. Refreshed periodically, and
/// at most every few seconds on a miss so a newly added dealership is picked up without a restart.
/// Loading itself is <see cref="ReferenceDataStore"/>'s job; this class only decides when.
/// </summary>
public sealed class ReferenceDataCache(ReferenceDataStore store, TimeProvider clock, IOptions<BookingOptions> options)
{
    private static readonly TimeSpan MissRefreshInterval = TimeSpan.FromSeconds(5);
    private readonly SemaphoreSlim _reload = new(1, 1);
    private ReferenceSnapshot? _current;

    public async ValueTask<ReferenceSnapshot> GetAsync(CancellationToken ct)
    {
        var current = Volatile.Read(ref _current);
        if (current is not null && Age(current) < options.Value.ReferenceDataRefresh) return current;
        return await ReloadAsync(current, ct);
    }

    public async ValueTask<DealershipProfile?> FindDealershipAsync(int id, CancellationToken ct)
    {
        var snapshot = await GetAsync(ct);
        if (snapshot.Dealerships.TryGetValue(id, out var profile)) return profile;
        if (Age(snapshot) < MissRefreshInterval) return null;
        return (await ReloadAsync(snapshot, ct)).Dealerships.GetValueOrDefault(id);
    }

    public async ValueTask<ServiceType?> FindServiceTypeAsync(string code, CancellationToken ct)
    {
        var snapshot = await GetAsync(ct);
        if (snapshot.ServiceTypes.TryGetValue(code, out var service)) return service;
        if (Age(snapshot) < MissRefreshInterval) return null;
        return (await ReloadAsync(snapshot, ct)).ServiceTypes.GetValueOrDefault(code);
    }

    private TimeSpan Age(ReferenceSnapshot snapshot) => clock.GetUtcNow().UtcDateTime - snapshot.LoadedAt;

    private async Task<ReferenceSnapshot> ReloadAsync(ReferenceSnapshot? seen, CancellationToken ct)
    {
        await _reload.WaitAsync(ct);
        try
        {
            // Another caller may have reloaded while this one waited.
            if (!ReferenceEquals(Volatile.Read(ref _current), seen)) return _current!;
            var loaded = await LoadAsync(ct);
            Volatile.Write(ref _current, loaded);
            return loaded;
        }
        finally
        {
            _reload.Release();
        }
    }

    private async Task<ReferenceSnapshot> LoadAsync(CancellationToken ct)
    {
        var (dealerships, services) = await store.LoadAllAsync(ct);
        return new ReferenceSnapshot(dealerships, services, clock.GetUtcNow().UtcDateTime);
    }
}
