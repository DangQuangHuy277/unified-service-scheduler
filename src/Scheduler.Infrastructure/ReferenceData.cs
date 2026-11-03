using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Scheduler.Domain;
using Scheduler.Infrastructure.Persistence;

namespace Scheduler.Infrastructure;

public sealed record ReferenceSnapshot(
    IReadOnlyDictionary<int, DealershipProfile> Dealerships,
    IReadOnlyDictionary<string, ServiceType> ServiceTypes,
    DateTime LoadedAt);

/// <summary>
/// Dealerships, their technicians, bays and hours, and service types: small, read-mostly data that both the
/// API (to validate before enqueuing) and the workers (to decide) keep in memory. Refreshed periodically, and
/// at most every few seconds on a miss so a newly added dealership is picked up without a restart.
/// </summary>
public sealed class ReferenceDataCache(IServiceScopeFactory scopes, TimeProvider clock, IOptions<BookingOptions> options)
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
        await using var scope = scopes.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<SchedulerDbContext>();
        var dealerships = await db.Dealerships.AsNoTracking()
            .Include(d => d.OpeningHours).Include(d => d.Technicians).Include(d => d.Bays)
            .AsSplitQuery().ToListAsync(ct);
        var services = await db.ServiceTypes.AsNoTracking().ToListAsync(ct);

        return new ReferenceSnapshot(
            dealerships.ToDictionary(d => d.Id, ToProfile),
            services.ToDictionary(
                s => s.Code,
                s => new ServiceType(s.Code, s.Name, TimeSpan.FromMinutes(s.DurationMinutes), s.RequiredSkill),
                StringComparer.OrdinalIgnoreCase),
            clock.GetUtcNow().UtcDateTime);
    }

    private static DealershipProfile ToProfile(DealershipRow d) => new(
        d.Id,
        d.Name,
        TimeZoneInfo.FindSystemTimeZoneById(d.TimeZone),
        new OpeningHours(d.OpeningHours.ToDictionary(h => h.Weekday, h => (h.Opens, h.Closes))),
        d.Technicians.Select(t => new Technician(t.Id, t.Name, t.Skills.ToHashSet(StringComparer.OrdinalIgnoreCase))).ToArray(),
        d.Bays.Select(b => new Bay(b.Id, b.Name)).ToArray());
}
