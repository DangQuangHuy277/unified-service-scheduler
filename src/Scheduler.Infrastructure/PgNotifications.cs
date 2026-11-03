using Microsoft.Extensions.Logging;
using Npgsql;

namespace Scheduler.Infrastructure;

public static class PgChannels
{
    /// <summary>Payload: comma-separated request ids whose decision just committed.</summary>
    public const string BookingDecided = "booking_decided";

    /// <summary>No payload; wakes the outbox relay.</summary>
    public const string OutboxPending = "outbox_pending";
}

/// <summary>
/// Postgres LISTEN on one dedicated connection, reconnecting after failures. NOTIFY sent inside a
/// transaction is delivered only when it commits, which is what the reply path relies on (D-006).
/// </summary>
public static class PgListener
{
    public static async Task RunAsync(NpgsqlDataSource db, string channel, Action<string> onPayload,
        ILogger log, CancellationToken ct, Action? onListening = null)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                await using var conn = await db.OpenConnectionAsync(ct);
                conn.Notification += (_, e) => onPayload(e.Payload);
                await using (var listen = new NpgsqlCommand($"LISTEN {channel}", conn))
                    await listen.ExecuteNonQueryAsync(ct);
                onListening?.Invoke();
                while (!ct.IsCancellationRequested) await conn.WaitAsync(ct);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                return;
            }
            catch (Exception e)
            {
                log.LogWarning(e, "LISTEN {Channel} connection lost; reconnecting", channel);
                try { await Task.Delay(TimeSpan.FromSeconds(1), ct); }
                catch (OperationCanceledException) { return; }
            }
        }
    }
}
