using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Scheduler.Infrastructure;
using Scheduler.Infrastructure.Persistence;
using Scheduler.IntegrationTests.Infrastructure;

namespace Scheduler.IntegrationTests;

/// <summary>The compose stack runs `--migrate --seed` on every start, so the seed must be repeatable and loadable.</summary>
public sealed class DemoDataTests(Containers containers)
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task Demo_seed_is_repeatable_and_loads_as_reference_data()
    {
        await using var env = await TestEnvironment.CreateAsync(containers, _ => Task.CompletedTask, startWorkers: false);
        await using var db = new SchedulerDbContext(new DbContextOptionsBuilder<SchedulerDbContext>()
            .UseNpgsql(env.ConnectionString).UseSnakeCaseNamingConvention().Options);

        await DemoData.SeedAsync(db, Ct);
        await DemoData.SeedAsync(db, Ct);   // second run: ON CONFLICT DO NOTHING everywhere

        var snapshot = await env.BuildWorkerServices().GetRequiredService<ReferenceDataCache>().GetAsync(Ct);
        Assert.Equal(3, snapshot.Dealerships.Count);
        Assert.Equal(6, snapshot.ServiceTypes.Count);
        var london = snapshot.Dealerships[1];
        Assert.Equal("Europe/London", london.TimeZone.Id);
        Assert.Contains(london.Technicians, t => t.Id == 103 && t.Skills.Contains("ev"));
        Assert.False(london.Hours.Days.ContainsKey(DayOfWeek.Sunday));
    }
}
