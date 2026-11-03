using System.Diagnostics;
using Microsoft.Extensions.Options;
using Npgsql;
using NpgsqlTypes;
using Scheduler.Domain;
using Scheduler.Infrastructure;
using Scheduler.Infrastructure.Messaging;

namespace Scheduler.Worker.Booking;

/// <summary>A command as received from Kafka: when it was enqueued and the trace it belongs to.</summary>
public sealed record CommandEnvelope(BookAppointmentCommand Command, DateTime EnqueuedAt, ActivityContext TraceParent);

public sealed record DecidedRequest(Guid RequestId, BookingDecision Decision);

public sealed record BatchResult(IReadOnlyList<DecidedRequest> Decided, IReadOnlyList<Guid> Duplicates);

/// <summary>
/// Decides one partition's batch and writes it in ONE transaction:
/// fence check → skip already-decided ids → decide in memory → booking_request + customer/vehicle upserts +
/// appointment + outbox rows → NOTIFY. Kafka offsets are committed only after this returns (at-least-once
/// delivery, exactly-once effect). Called only from the thread that owns the partition.
/// </summary>
public sealed class BookingBatchProcessor(
    NpgsqlDataSource db,
    ReferenceDataCache reference,
    TimeProvider clock,
    IOptions<KafkaOptions> kafka,
    ILogger<BookingBatchProcessor> log)
{
    public async Task<BatchResult> ProcessAsync(PartitionState partition, IReadOnlyList<CommandEnvelope> batch, CancellationToken ct)
    {
        using var activity = SchedulerTelemetry.Source.StartActivity(
            $"{kafka.Value.BookingTopic} process", ActivityKind.Consumer, parentContext: default,
            links: batch.Where(e => e.TraceParent != default).Select(e => new ActivityLink(e.TraceParent)));
        activity?.SetTag("messaging.destination.partition.id", partition.Partition);
        activity?.SetTag("messaging.batch.message_count", batch.Count);
        activity?.SetTag("scheduler.partition.epoch", partition.Epoch);

        var decided = new List<(CommandEnvelope Envelope, BookingDecision Decision)>(batch.Count);
        var duplicates = new List<Guid>();
        try
        {
            await using var conn = await db.OpenConnectionAsync(ct);
            await using var tx = await conn.BeginTransactionAsync(ct);
            await PartitionLeaseStore.CheckAsync(conn, tx, kafka.Value.BookingTopic, partition, ct);

            var alreadyDecided = await AlreadyDecidedAsync(conn, tx, batch, ct);
            var now = clock.GetUtcNow().UtcDateTime;
            var seen = new HashSet<Guid>();
            foreach (var envelope in batch)
            {
                var id = envelope.Command.RequestId;
                // A redelivery after a crash, or a client retrying with the same Idempotency-Key.
                if (alreadyDecided.Contains(id) || !seen.Add(id)) { duplicates.Add(id); continue; }
                decided.Add((envelope, await DecideAsync(conn, tx, partition, envelope.Command, now, ct)));
            }

            await BookingBatchWriter.WriteAsync(conn, tx, decided, duplicates, kafka.Value.EventsTopic, now, ct);
            await tx.CommitAsync(ct);
        }
        catch (Exception e)
        {
            // Reservations made in memory never committed: drop those schedules so they reload from the database.
            foreach (var (envelope, _) in decided) partition.Schedules.Remove(envelope.Command.DealershipId);
            if (e is PostgresException { SqlState: PostgresErrorCodes.ExclusionViolation } violation)
            {
                SchedulerTelemetry.BackstopViolations.Add(1);
                log.LogError(violation, "Exclusion constraint rejected a batch on partition {Partition}: two writers for one dealership?",
                    partition.Partition);
            }
            activity?.SetStatus(ActivityStatusCode.Error, e.Message);
            throw;
        }

        var committedAt = clock.GetUtcNow().UtcDateTime;
        SchedulerTelemetry.BatchSize.Record(batch.Count);
        foreach (var (envelope, decision) in decided)
        {
            SchedulerTelemetry.Decisions.Add(1, new KeyValuePair<string, object?>("outcome", decision.Outcome.ToString()));
            SchedulerTelemetry.DecisionLatency.Record((committedAt - envelope.EnqueuedAt).TotalMilliseconds);
            RecordDecisionSpan(envelope, decision, committedAt);
        }
        return new BatchResult(decided.Select(d => new DecidedRequest(d.Envelope.Command.RequestId, d.Decision)).ToArray(), duplicates);
    }

    private async Task<BookingDecision> DecideAsync(NpgsqlConnection conn, NpgsqlTransaction tx, PartitionState partition,
        BookAppointmentCommand command, DateTime now, CancellationToken ct)
    {
        var profile = await reference.FindDealershipAsync(command.DealershipId, ct);
        if (profile is null) return BookingDecision.Rejected(BookingOutcome.UnknownDealership);
        var service = await reference.FindServiceTypeAsync(command.ServiceTypeCode, ct);
        if (service is null) return BookingDecision.Rejected(BookingOutcome.UnknownServiceType);

        // Reload when reference data changed (new technician, new hours): the schedule holds the profile it was built from.
        if (!partition.Schedules.TryGetValue(profile.Id, out var schedule) || !ReferenceEquals(schedule.Profile, profile))
            partition.Schedules[profile.Id] = schedule = await LoadScheduleAsync(conn, tx, profile, now, ct);

        return schedule.TryReserve(service, command.Start, now);
    }

    private static async Task<DealershipSchedule> LoadScheduleAsync(NpgsqlConnection conn, NpgsqlTransaction tx,
        DealershipProfile profile, DateTime now, CancellationToken ct)
    {
        var started = Stopwatch.GetTimestamp();
        var schedule = new DealershipSchedule(profile);
        await using var cmd = new NpgsqlCommand("""
            SELECT technician_id, bay_id, lower(slot), upper(slot)
              FROM appointment
             WHERE dealership_id = $1 AND slot && tstzrange($2, NULL)
            """, conn, tx);
        cmd.Parameters.Add(new() { Value = profile.Id });
        cmd.Parameters.Add(new() { Value = now });
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
            schedule.Add(reader.GetInt32(0), reader.GetInt32(1), new TimeSlot(reader.GetDateTime(2), reader.GetDateTime(3)));
        SchedulerTelemetry.ScheduleLoad.Record(Stopwatch.GetElapsedTime(started).TotalMilliseconds);
        return schedule;
    }

    private static async Task<HashSet<Guid>> AlreadyDecidedAsync(NpgsqlConnection conn, NpgsqlTransaction tx,
        IReadOnlyList<CommandEnvelope> batch, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand("SELECT id FROM booking_request WHERE id = ANY($1)", conn, tx);
        cmd.Parameters.Add(new() { Value = batch.Select(e => e.Command.RequestId).ToArray(), NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Uuid });
        var ids = new HashSet<Guid>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct)) ids.Add(reader.GetGuid(0));
        return ids;
    }

    /// <summary>One short span per request, parented to the API's publish span, so a single trace runs API → decision.</summary>
    private static void RecordDecisionSpan(CommandEnvelope envelope, BookingDecision decision, DateTime committedAt)
    {
        if (envelope.TraceParent == default) return;
        using var span = SchedulerTelemetry.Source.StartActivity("booking decide", ActivityKind.Internal,
            envelope.TraceParent, startTime: envelope.EnqueuedAt);
        if (span is null) return;
        span.SetTag("scheduler.request_id", envelope.Command.RequestId);
        span.SetTag("scheduler.dealership_id", envelope.Command.DealershipId);
        span.SetTag("scheduler.outcome", decision.Outcome.ToString());
        span.SetEndTime(committedAt);
    }
}
