using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Scheduler.Domain;
using Scheduler.Infrastructure.Persistence;

namespace Scheduler.Infrastructure.Data;

/// <summary>
/// Loads all reference data (dealerships with hours, technicians and bays; service types) and maps it to domain
/// types. EF Core: a small object graph read every few minutes, where readability matters more than round trips.
/// </summary>
public sealed class ReferenceDataStore(IServiceScopeFactory scopes)
{
    public async Task<(IReadOnlyDictionary<int, DealershipProfile> Dealerships, IReadOnlyDictionary<string, ServiceType> ServiceTypes)>
        LoadAllAsync(CancellationToken ct)
    {
        // A DbContext is not thread-safe, so each load gets its own scope.
        await using var scope = scopes.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<SchedulerDbContext>();
        var dealerships = await db.Dealerships.AsNoTracking()
            .Include(d => d.OpeningHours).Include(d => d.Technicians).Include(d => d.Bays)
            .AsSplitQuery().ToListAsync(ct);
        var services = await db.ServiceTypes.AsNoTracking().ToListAsync(ct);

        return (
            dealerships.ToDictionary(d => d.Id, ToProfile),
            services.ToDictionary(
                s => s.Code,
                s => new ServiceType(s.Code, s.Name, TimeSpan.FromMinutes(s.DurationMinutes), s.RequiredSkill),
                StringComparer.OrdinalIgnoreCase));
    }

    private static DealershipProfile ToProfile(DealershipRow d) => new(
        d.Id,
        d.Name,
        TimeZoneInfo.FindSystemTimeZoneById(d.TimeZone),
        new OpeningHours(d.OpeningHours.ToDictionary(h => h.Weekday, h => (h.Opens, h.Closes))),
        d.Technicians.Select(t => new Technician(t.Id, t.Name, t.Skills.ToHashSet(StringComparer.OrdinalIgnoreCase))).ToArray(),
        d.Bays.Select(b => new Bay(b.Id, b.Name)).ToArray());
}
