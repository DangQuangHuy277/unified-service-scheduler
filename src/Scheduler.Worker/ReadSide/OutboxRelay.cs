using System.Threading.Channels;
using Confluent.Kafka;
using Npgsql;
using Scheduler.Infrastructure;
using Scheduler.Infrastructure.Data;
using Scheduler.Infrastructure.Messaging;

namespace Scheduler.Worker.ReadSide;

/// <summary>
/// Publishes outbox rows to Kafka, then marks them published. Woken by NOTIFY when a batch commits, with a
/// 1 s poll as a fallback. <c>FOR UPDATE SKIP LOCKED</c> lets several workers relay without sending a row twice
/// in the normal case; a crash between producing and marking can still resend, so consumers are idempotent.
/// Events for one dealership can leave in a different order across relays; the projection doesn't depend on order.
/// </summary>
public sealed class OutboxRelay(NpgsqlDataSource db, OutboxStore outbox, KafkaProducer kafka, ILogger<OutboxRelay> log) : BackgroundService
{
    private const int BatchSize = 500;
    private readonly Channel<bool> _wake = Channel.CreateBounded<bool>(new BoundedChannelOptions(1) { FullMode = BoundedChannelFullMode.DropWrite });

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        _ = PgListener.RunAsync(db, PgChannels.OutboxPending, _ => _wake.Writer.TryWrite(true), log, ct);
        while (!ct.IsCancellationRequested)
        {
            try
            {
                while (await RelayOnceAsync(ct) == BatchSize) { }   // drain a backlog before waiting again
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                return;
            }
            catch (Exception e)
            {
                log.LogError(e, "Outbox relay failed; retrying");
            }

            using var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
            timeout.CancelAfter(TimeSpan.FromSeconds(1));
            try { await _wake.Reader.ReadAsync(timeout.Token); }
            catch (OperationCanceledException) when (!ct.IsCancellationRequested) { }
            catch (OperationCanceledException) { return; }
        }
    }

    private async Task<int> RelayOnceAsync(CancellationToken ct)
    {
        await using var conn = await db.OpenConnectionAsync(ct);
        await using var tx = await conn.BeginTransactionAsync(ct);

        var rows = await outbox.ClaimBatchAsync(conn, tx, BatchSize, ct);
        if (rows.Count > 0)
        {
            // Wait for every delivery: a row is only marked once Kafka has it.
            await Task.WhenAll(rows.Select(r => kafka.Producer.ProduceAsync(r.Topic, new Message<string, byte[]>
            {
                Key = r.Key,
                Value = System.Text.Encoding.UTF8.GetBytes(r.Payload),
                Headers = new Headers { { "type", System.Text.Encoding.UTF8.GetBytes(MessageJson.AppointmentConfirmed) } },
            }, ct)));

            await outbox.MarkPublishedAsync(conn, tx, rows.Select(r => r.Id).ToArray(), ct);
        }
        await tx.CommitAsync(ct);

        SchedulerTelemetry.ReportOutboxBacklog(rows.Count < BatchSize ? 0 : await outbox.CountUnpublishedAsync(ct));
        return rows.Count;
    }
}
