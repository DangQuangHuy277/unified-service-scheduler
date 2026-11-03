using System.Threading.Channels;
using Confluent.Kafka;
using Npgsql;
using Scheduler.Infrastructure;
using Scheduler.Infrastructure.Messaging;

namespace Scheduler.Worker.ReadSide;

/// <summary>
/// Publishes outbox rows to Kafka, then marks them published. Woken by NOTIFY when a batch commits, with a
/// 1 s poll as a fallback. <c>FOR UPDATE SKIP LOCKED</c> lets several workers relay without sending a row twice
/// in the normal case; a crash between producing and marking can still resend, so consumers are idempotent.
/// Events for one dealership can leave in a different order across relays; the projection doesn't depend on order.
/// </summary>
public sealed class OutboxRelay(NpgsqlDataSource db, KafkaProducer kafka, ILogger<OutboxRelay> log) : BackgroundService
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

        var rows = new List<(long Id, string Topic, string Key, string Payload)>();
        await using (var select = new NpgsqlCommand("""
            SELECT id, topic, key, payload::text FROM outbox_message
             WHERE published_at IS NULL
             ORDER BY id
             LIMIT $1
               FOR UPDATE SKIP LOCKED
            """, conn, tx))
        {
            select.Parameters.Add(new() { Value = BatchSize });
            await using var reader = await select.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
                rows.Add((reader.GetInt64(0), reader.GetString(1), reader.GetString(2), reader.GetString(3)));
        }

        if (rows.Count > 0)
        {
            // Wait for every delivery: a row is only marked once Kafka has it.
            await Task.WhenAll(rows.Select(r => kafka.Producer.ProduceAsync(r.Topic, new Message<string, byte[]>
            {
                Key = r.Key,
                Value = System.Text.Encoding.UTF8.GetBytes(r.Payload),
                Headers = new Headers { { "type", System.Text.Encoding.UTF8.GetBytes(MessageJson.AppointmentConfirmed) } },
            }, ct)));

            await using var mark = new NpgsqlCommand("UPDATE outbox_message SET published_at = now() WHERE id = ANY($1)", conn, tx);
            mark.Parameters.Add(new() { Value = rows.Select(r => r.Id).ToArray() });
            await mark.ExecuteNonQueryAsync(ct);
        }
        await tx.CommitAsync(ct);

        SchedulerTelemetry.ReportOutboxBacklog(rows.Count < BatchSize ? 0 : await CountBacklogAsync(ct));
        return rows.Count;
    }

    private async Task<long> CountBacklogAsync(CancellationToken ct)
    {
        await using var cmd = db.CreateCommand("SELECT count(*) FROM outbox_message WHERE published_at IS NULL");
        return (long)(await cmd.ExecuteScalarAsync(ct))!;
    }
}
