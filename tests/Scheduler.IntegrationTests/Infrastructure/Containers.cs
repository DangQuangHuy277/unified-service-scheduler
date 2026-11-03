using Npgsql;
using Testcontainers.Kafka;
using Testcontainers.PostgreSql;

[assembly: AssemblyFixture(typeof(Scheduler.IntegrationTests.Infrastructure.Containers))]

namespace Scheduler.IntegrationTests.Infrastructure;

/// <summary>
/// One Postgres and one Kafka (KRaft, the official Apache image) for the whole test run. Each test gets its own
/// database, topics and consumer groups from <see cref="TestEnvironment"/>, so tests stay independent.
/// </summary>
public sealed class Containers : IAsyncLifetime
{
    public const string KafkaImage = "apache/kafka:3.9.1";
    public const string PostgresImage = "postgres:16-alpine";

    private readonly PostgreSqlContainer _postgres = new PostgreSqlBuilder(PostgresImage).Build();

    private readonly KafkaContainer _kafka = new KafkaBuilder(KafkaImage)
        // Without this the broker waits 3 s before the first consumer-group rebalance.
        .WithEnvironment("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0")
        .Build();

    public string KafkaBootstrap => _kafka.GetBootstrapAddress();

    public async ValueTask InitializeAsync() => await Task.WhenAll(_postgres.StartAsync(), _kafka.StartAsync());

    public async Task<string> CreateDatabaseAsync(string name)
    {
        await using var admin = new NpgsqlConnection(_postgres.GetConnectionString());
        await admin.OpenAsync();
        await using (var cmd = new NpgsqlCommand($"CREATE DATABASE \"{name}\"", admin)) await cmd.ExecuteNonQueryAsync();
        return new NpgsqlConnectionStringBuilder(_postgres.GetConnectionString()) { Database = name }.ConnectionString;
    }

    public async ValueTask DisposeAsync()
    {
        await _kafka.DisposeAsync();
        await _postgres.DisposeAsync();
    }
}
