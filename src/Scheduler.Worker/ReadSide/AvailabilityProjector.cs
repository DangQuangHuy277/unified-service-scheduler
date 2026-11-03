using Confluent.Kafka;
using Microsoft.Extensions.Options;
using Npgsql;
using Scheduler.Infrastructure;
using Scheduler.Infrastructure.Messaging;

namespace Scheduler.Worker.ReadSide;

/// <summary>
/// Builds the availability read model (<c>read.availability_busy</c>) from AppointmentConfirmed events.
/// Idempotent by appointment id, so redelivered or re-relayed events are harmless; offsets are committed
/// after the rows are written. In production this table would live in a read replica or a separate store.
/// </summary>
public sealed class AvailabilityProjector(
    NpgsqlDataSource db,
    TimeProvider clock,
    IOptions<KafkaOptions> kafkaOptions,
    IOptions<WorkerOptions> workerOptions,
    ILogger<AvailabilityProjector> log) : BackgroundService
{
    private const int BatchSize = 500;
    private readonly KafkaOptions _kafka = kafkaOptions.Value;

    protected override Task ExecuteAsync(CancellationToken ct) =>
        Task.Factory.StartNew(() => Run(ct), ct, TaskCreationOptions.LongRunning, TaskScheduler.Default);

    private void Run(CancellationToken ct)
    {
        using var consumer = new ConsumerBuilder<string, byte[]>(new ConsumerConfig
        {
            BootstrapServers = _kafka.BootstrapServers,
            GroupId = _kafka.ProjectorGroup,
            ClientId = $"{workerOptions.Value.InstanceId}-projector",
            EnableAutoCommit = false,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            PartitionAssignmentStrategy = PartitionAssignmentStrategy.CooperativeSticky,
            SessionTimeoutMs = _kafka.SessionTimeoutMs,
        }).SetErrorHandler((_, e) => log.LogWarning("Kafka consumer error: {Reason}", e.Reason)).Build();
        consumer.Subscribe(_kafka.EventsTopic);

        try
        {
            while (!ct.IsCancellationRequested)
            {
                var polled = new List<ConsumeResult<string, byte[]>> { consumer.Consume(ct) };
                while (polled.Count < BatchSize && consumer.Consume(TimeSpan.Zero) is { } next) polled.Add(next);

                var events = new List<AppointmentConfirmedEvent>(polled.Count);
                foreach (var result in polled)
                {
                    try { events.Add(MessageJson.Deserialize<AppointmentConfirmedEvent>(result.Message.Value)); }
                    catch (Exception e) { log.LogError(e, "Skipping malformed event at {Offset}", result.TopicPartitionOffset); }
                }

                try
                {
                    if (events.Count > 0) ProjectAsync(events, ct).GetAwaiter().GetResult();
                    consumer.Commit(polled.GroupBy(r => r.TopicPartition)
                        .Select(g => new TopicPartitionOffset(g.Key, g.Max(r => r.Offset.Value) + 1)));
                }
                catch (Exception e) when (!ct.IsCancellationRequested)
                {
                    log.LogError(e, "Projection failed; replaying the batch");
                    foreach (var first in polled.GroupBy(r => r.TopicPartition).Select(g => g.MinBy(r => r.Offset.Value)!))
                        consumer.Seek(first.TopicPartitionOffset);
                    ct.WaitHandle.WaitOne(TimeSpan.FromMilliseconds(500));
                }
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
        }
        finally
        {
            consumer.Close();
        }
    }

    private async Task ProjectAsync(List<AppointmentConfirmedEvent> events, CancellationToken ct)
    {
        await using var cmd = db.CreateCommand($"""
            INSERT INTO read.availability_busy (appointment_id, dealership_id, technician_id, bay_id, slot, confirmed_at, projected_at)
            SELECT a, d, t, b, tstzrange(s, e, '[)'), c, now()
              FROM unnest($1::bigint[], $2::int[], $3::int[], $4::int[], $5::timestamptz[], $6::timestamptz[], $7::timestamptz[])
                   AS x(a, d, t, b, s, e, c)
            ON CONFLICT (appointment_id) DO NOTHING
            """);
        cmd.Parameters.Add(new() { Value = events.Select(e => e.AppointmentId).ToArray() });
        cmd.Parameters.Add(new() { Value = events.Select(e => e.DealershipId).ToArray() });
        cmd.Parameters.Add(new() { Value = events.Select(e => e.TechnicianId).ToArray() });
        cmd.Parameters.Add(new() { Value = events.Select(e => e.BayId).ToArray() });
        cmd.Parameters.Add(new() { Value = events.Select(e => e.Start).ToArray() });
        cmd.Parameters.Add(new() { Value = events.Select(e => e.End).ToArray() });
        cmd.Parameters.Add(new() { Value = events.Select(e => e.ConfirmedAt).ToArray() });
        await cmd.ExecuteNonQueryAsync(ct);

        var now = clock.GetUtcNow().UtcDateTime;
        foreach (var e in events) SchedulerTelemetry.ProjectionLag.Record((now - e.ConfirmedAt).TotalMilliseconds);
    }
}
