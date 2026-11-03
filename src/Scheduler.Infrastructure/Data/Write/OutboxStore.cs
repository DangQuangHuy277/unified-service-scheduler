using Npgsql;

namespace Scheduler.Infrastructure.Data;

public sealed record OutboxRow(long Id, string Topic, string Key, string Payload);

/// <summary>
/// The relay's view of <c>outbox_message</c>. Claim and mark run in the caller's transaction, so a claimed row
/// stays locked while it is being published and is released if publishing fails.
/// </summary>
public sealed class OutboxStore(NpgsqlDataSource db)
{
    /// <summary>
    /// Oldest unpublished rows, locked. <c>SKIP LOCKED</c> lets several relays work at once without
    /// claiming the same row.
    /// </summary>
    public async Task<IReadOnlyList<OutboxRow>> ClaimBatchAsync(NpgsqlConnection conn, NpgsqlTransaction tx, int limit,
        CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand("""
            SELECT id, topic, key, payload::text FROM outbox_message
             WHERE published_at IS NULL
             ORDER BY id
             LIMIT $1
               FOR UPDATE SKIP LOCKED
            """, conn, tx);
        cmd.Parameters.Add(new() { Value = limit });
        var rows = new List<OutboxRow>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
            rows.Add(new OutboxRow(reader.GetInt64(0), reader.GetString(1), reader.GetString(2), reader.GetString(3)));
        return rows;
    }

    public async Task MarkPublishedAsync(NpgsqlConnection conn, NpgsqlTransaction tx, IReadOnlyCollection<long> ids,
        CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand("UPDATE outbox_message SET published_at = now() WHERE id = ANY($1)", conn, tx);
        cmd.Parameters.Add(new() { Value = ids.ToArray() });
        await cmd.ExecuteNonQueryAsync(ct);
    }

    public async Task<long> CountUnpublishedAsync(CancellationToken ct)
    {
        await using var cmd = db.CreateCommand("SELECT count(*) FROM outbox_message WHERE published_at IS NULL");
        return (long)(await cmd.ExecuteScalarAsync(ct))!;
    }
}
