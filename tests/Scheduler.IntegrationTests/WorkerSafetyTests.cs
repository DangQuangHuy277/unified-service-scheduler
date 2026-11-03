using System.Diagnostics;
using System.Net;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Npgsql;
using Scheduler.Infrastructure;
using Scheduler.Infrastructure.Data;
using Scheduler.Infrastructure.Messaging;
using Scheduler.IntegrationTests.Infrastructure;
using Scheduler.Worker.Booking;
using static Scheduler.IntegrationTests.Infrastructure.TestData;

namespace Scheduler.IntegrationTests;

/// <summary>
/// The failure modes a single-writer design has to survive: redelivery after a crash, a stale worker that
/// still believes it owns a partition, and a worker leaving mid-traffic. Plus the database backstop itself.
/// </summary>
public sealed class WorkerSafetyTests(Containers containers)
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static Task Seed(NpgsqlDataSource db) => Task.WhenAll(SeedServicesAsync(db), SeedDealershipAsync(db, 1, General(2), bays: 2));

    [Fact]
    public async Task Replaying_a_batch_after_a_crash_does_not_book_twice()
    {
        await using var env = await TestEnvironment.CreateAsync(containers, Seed, startWorkers: false);
        var services = env.BuildWorkerServices();
        var processor = services.GetRequiredService<BookingBatchProcessor>();
        var leases = services.GetRequiredService<LeaseStore>();
        var topic = services.GetRequiredService<IOptions<KafkaOptions>>().Value.BookingTopic;
        var batch = new[] { Envelope(Guid.CreateVersion7(), At(10)) };

        var firstOwner = new PartitionState(0, await leases.AcquireAsync(topic, 0, "first", Ct));
        var first = await processor.ProcessAsync(firstOwner, batch, Ct);
        // The first owner crashed before committing its Kafka offset; the next owner receives the same message.
        var nextOwner = new PartitionState(0, await leases.AcquireAsync(topic, 0, "next", Ct));
        var replay = await processor.ProcessAsync(nextOwner, batch, Ct);

        Assert.True(first.Decided.Single().Decision.IsConfirmed);
        Assert.Empty(replay.Decided);
        Assert.Equal(batch[0].Command.RequestId, replay.Duplicates.Single());
        Assert.Equal(1, await CountAsync(env.Db, "appointment"));
        Assert.Equal(1, await CountAsync(env.Db, "outbox_message"));
    }

    [Fact]
    public async Task A_worker_that_lost_its_partition_cannot_write()
    {
        await using var env = await TestEnvironment.CreateAsync(containers, Seed, startWorkers: false);
        var services = env.BuildWorkerServices();
        var processor = services.GetRequiredService<BookingBatchProcessor>();
        var leases = services.GetRequiredService<LeaseStore>();
        var topic = services.GetRequiredService<IOptions<KafkaOptions>>().Value.BookingTopic;

        var stale = new PartitionState(0, await leases.AcquireAsync(topic, 0, "stale", Ct));
        var current = new PartitionState(0, await leases.AcquireAsync(topic, 0, "current", Ct));

        // Both believe they can book the same slot; only the current owner's write may land.
        await Assert.ThrowsAsync<PartitionLostException>(() =>
            processor.ProcessAsync(stale, [Envelope(Guid.CreateVersion7(), At(10))], Ct));
        var accepted = await processor.ProcessAsync(current, [Envelope(Guid.CreateVersion7(), At(10))], Ct);

        Assert.True(accepted.Decided.Single().Decision.IsConfirmed);
        Assert.Equal(1, await CountAsync(env.Db, "appointment"));
        Assert.Equal(0, await CountAsync(env.Db, "booking_request WHERE outcome <> 'Confirmed'"));
    }

    [Fact]
    public async Task Bookings_continue_when_a_worker_leaves_and_its_partitions_move()
    {
        await using var env = await TestEnvironment.CreateAsync(containers, async db =>
        {
            await SeedServicesAsync(db);
            for (var d = 1; d <= 8; d++) await SeedDealershipAsync(db, d, General(2), bays: 2);
        }, workers: 2);
        // Cooperative rebalancing may take a moment to spread partitions over both workers.
        await env.WaitUntilAllPartitionsOwnedAsync(() => env.Workers.All(w => TestEnvironment.OwnedBy(w).Count > 0));
        var api = env.Api;

        var before = await Task.WhenAll(Enumerable.Range(1, 8).Select(d => api.BookAsync(Request(d, At(9), customer: d))));
        await env.StopWorkerAsync(env.Workers[0]);
        await env.WaitUntilAllPartitionsOwnedAsync();
        var after = await Task.WhenAll(Enumerable.Range(1, 8).Select(d => api.BookAsync(Request(d, At(11), customer: d + 100))));

        Assert.All(before.Concat(after), r => Assert.Equal(HttpStatusCode.Created, r.StatusCode));
        Assert.Equal(16, await CountAsync(env.Db, "appointment"));
        Assert.Equal(0, await CountOverlapsAsync(env.Db));
    }

    [Fact]
    public async Task The_database_rejects_an_overlap_even_if_application_code_tries_one()
    {
        await using var env = await TestEnvironment.CreateAsync(containers, Seed, startWorkers: false);
        await using var cmd = env.Db.CreateCommand("""
            INSERT INTO booking_request (id, dealership_id, service_type_code, requested_start, outcome, received_at, decided_at)
            VALUES ('00000000-0000-0000-0000-000000000001', 1, 'OIL', now(), 'Confirmed', now(), now()),
                   ('00000000-0000-0000-0000-000000000002', 1, 'OIL', now(), 'Confirmed', now(), now());
            INSERT INTO customer (email, name) VALUES ('a@example.com', 'A');
            INSERT INTO vehicle (vin, make, model, customer_id) VALUES ('JTDBR32E000000001', 'Toyota', 'Corolla', 1);
            INSERT INTO appointment (booking_request_id, dealership_id, customer_id, vehicle_id, service_type_code, technician_id, bay_id, slot)
            VALUES ('00000000-0000-0000-0000-000000000001', 1, 1, 1, 'OIL', 101, 151, tstzrange('2030-01-01 09:00Z', '2030-01-01 10:00Z', '[)'));
            """);
        await cmd.ExecuteNonQueryAsync(Ct);

        // Same technician, different bay, overlapping by 30 minutes.
        await using var overlap = env.Db.CreateCommand("""
            INSERT INTO appointment (booking_request_id, dealership_id, customer_id, vehicle_id, service_type_code, technician_id, bay_id, slot)
            VALUES ('00000000-0000-0000-0000-000000000002', 1, 1, 1, 'OIL', 101, 152, tstzrange('2030-01-01 09:30Z', '2030-01-01 10:30Z', '[)'))
            """);
        var error = await Assert.ThrowsAsync<PostgresException>(() => overlap.ExecuteNonQueryAsync(Ct));

        Assert.Equal(PostgresErrorCodes.ExclusionViolation, error.SqlState);
        Assert.Equal("no_technician_overlap", error.ConstraintName);
    }

    private static CommandEnvelope Envelope(Guid requestId, DateTimeOffset start, int customer = 1) => new(
        new BookAppointmentCommand(requestId, 1, Oil, start.UtcDateTime,
            new CustomerDetails($"Customer {customer}", $"customer{customer}@example.com"),
            new VehicleDetails(Vin(customer), "Toyota", "Corolla"), DateTime.UtcNow),
        DateTime.UtcNow, default(ActivityContext));
}
