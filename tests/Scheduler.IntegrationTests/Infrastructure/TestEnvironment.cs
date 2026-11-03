using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Npgsql;
using Scheduler.Infrastructure.Persistence;
using Scheduler.Worker;
using Scheduler.Worker.Booking;

namespace Scheduler.IntegrationTests.Infrastructure;

/// <summary>
/// A full system for one test: its own database (migrated), its own Kafka topics and groups, N worker hosts
/// and an in-memory API. Workers are started before returning, and every partition has an owner.
/// </summary>
public sealed class TestEnvironment : IAsyncDisposable
{
    private readonly Dictionary<string, string?> _settings;
    private readonly List<IHost> _workers = [];
    private WebApplicationFactory<Program>? _api;

    private TestEnvironment(Dictionary<string, string?> settings, NpgsqlDataSource db)
    {
        _settings = settings;
        Db = db;
    }

    public const int Partitions = 4;

    public NpgsqlDataSource Db { get; }
    public IReadOnlyList<IHost> Workers => _workers;
    public string ConnectionString => _settings["ConnectionStrings:Scheduler"]!;

    public HttpClient Api => (_api ??= CreateApi()).CreateClient();

    public static async Task<TestEnvironment> CreateAsync(Containers containers, Func<NpgsqlDataSource, Task> seed,
        int workers = 1, bool startWorkers = true)
    {
        var name = $"t_{Guid.NewGuid():N}";
        var connectionString = await containers.CreateDatabaseAsync(name);
        var settings = new Dictionary<string, string?>
        {
            ["ConnectionStrings:Scheduler"] = connectionString,
            ["Kafka:BootstrapServers"] = containers.KafkaBootstrap,
            ["Kafka:BookingTopic"] = $"{name}-booking",
            ["Kafka:EventsTopic"] = $"{name}-events",
            ["Kafka:WorkerGroup"] = $"{name}-workers",
            ["Kafka:ProjectorGroup"] = $"{name}-projector",
            ["Kafka:Partitions"] = Partitions.ToString(),
            ["Kafka:SessionTimeoutMs"] = "6000",
            ["Logging:LogLevel:Default"] = "Warning",
        };

        await using (var db = new SchedulerDbContext(new DbContextOptionsBuilder<SchedulerDbContext>()
                         .UseNpgsql(connectionString).UseSnakeCaseNamingConvention().Options))
        {
            await db.Database.MigrateAsync();
        }

        var env = new TestEnvironment(settings, NpgsqlDataSource.Create(connectionString));
        await seed(env.Db);
        if (startWorkers)
        {
            for (var i = 0; i < workers; i++) await env.StartWorkerAsync();
            await env.WaitUntilAllPartitionsOwnedAsync();
        }
        return env;
    }

    /// <summary>A worker host built but not started: for driving the batch processor directly.</summary>
    public IServiceProvider BuildWorkerServices() => BuildWorker($"idle-{_workers.Count}").Services;

    public async Task<IHost> StartWorkerAsync()
    {
        var host = BuildWorker($"worker-{_workers.Count + 1}");
        await host.StartAsync();
        _workers.Add(host);
        return host;
    }

    public async Task StopWorkerAsync(IHost worker)
    {
        await worker.StopAsync();
        worker.Dispose();
        _workers.Remove(worker);
    }

    public static IReadOnlyCollection<int> OwnedBy(IHost worker) =>
        worker.Services.GetRequiredService<PartitionOwnership>().OwnedPartitions;

    public async Task WaitUntilAllPartitionsOwnedAsync(Func<bool>? andAlso = null)
    {
        await Eventually.TrueAsync(() =>
        {
            var owned = _workers.SelectMany(OwnedBy).ToArray();
            return owned.Length == Partitions && owned.Distinct().Count() == Partitions && (andAlso?.Invoke() ?? true);
        }, TimeSpan.FromSeconds(60), "every partition to have exactly one owner");
    }

    private IHost BuildWorker(string instance) =>
        WorkerProgram.CreateBuilder([], b => b.Configuration.AddInMemoryCollection(
            new Dictionary<string, string?>(_settings) { ["Worker:InstanceId"] = instance })).Build();

    private WebApplicationFactory<Program> CreateApi() => new WebApplicationFactory<Program>().WithWebHostBuilder(web =>
    {
        foreach (var (key, value) in _settings) web.UseSetting(key, value);
    });

    public async ValueTask DisposeAsync()
    {
        if (_api is not null) await _api.DisposeAsync();
        foreach (var worker in _workers.ToArray()) await StopWorkerAsync(worker);
        await Db.DisposeAsync();
    }
}

public static class Eventually
{
    public static async Task TrueAsync(Func<bool> condition, TimeSpan timeout, string what) =>
        await TrueAsync(() => Task.FromResult(condition()), timeout, what);

    public static async Task TrueAsync(Func<Task<bool>> condition, TimeSpan timeout, string what)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (await condition()) return;
            await Task.Delay(100);
        }
        throw new TimeoutException($"Timed out after {timeout.TotalSeconds:0} s waiting for {what}.");
    }
}
