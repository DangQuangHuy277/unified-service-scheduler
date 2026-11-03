using Npgsql;

namespace BookingConcurrency;

/// <summary>
/// O1: every request is its own transaction against Postgres. One statement picks the first free
/// technician and bay (first-fit, assumption A9) and inserts; the exclusion constraints reject any
/// overlap a concurrent request slipped in. Losers retry.
/// </summary>
/// <param name="lockDealershipDay">
/// false = "O1 naive": conflicting inserts race inside the constraint check.
/// true  = "O1 + lock": take pg_advisory_xact_lock(dealership, day) first, so requests for the
///         same dealership-day queue in the database instead of colliding.
/// </param>
public sealed class DatabaseEnforced(NpgsqlDataSource db, InFlightGauge gauge, bool lockDealershipDay) : IBookingStrategy
{
    private const int MaxAttempts = 5;

    private const string BookSql = """
        WITH t AS (
            SELECT id FROM technician
             WHERE dealership_id = $1
               AND NOT EXISTS (SELECT 1 FROM appointment a
                                WHERE a.dealership_id = $1 AND a.technician_id = technician.id
                                  AND a.slot && tstzrange($2, $3, '[)'))
             ORDER BY id LIMIT 1),
        b AS (
            SELECT id FROM bay
             WHERE dealership_id = $1
               AND NOT EXISTS (SELECT 1 FROM appointment a
                                WHERE a.dealership_id = $1 AND a.bay_id = bay.id
                                  AND a.slot && tstzrange($2, $3, '[)'))
             ORDER BY id LIMIT 1)
        INSERT INTO appointment (dealership_id, technician_id, bay_id, slot)
        SELECT $1, t.id, b.id, tstzrange($2, $3, '[)') FROM t, b
        RETURNING id
        """;

    public string Name => lockDealershipDay ? "o1-lock" : "o1-naive";

    public async Task<BookingResult> BookAsync(BookingRequest r, CancellationToken ct)
    {
        int conflicts = 0, deadlocks = 0;
        for (var attempt = 1; ; attempt++)
        {
            try
            {
                var booked = await TryOnceAsync(r, ct);
                return new BookingResult(booked ? Outcome.Confirmed : Outcome.NoCapacity,
                    DbTransactions: attempt, ConflictRetries: conflicts, DeadlockRetries: deadlocks);
            }
            catch (PostgresException e) when (e.SqlState is PostgresErrorCodes.ExclusionViolation
                                                         or PostgresErrorCodes.DeadlockDetected)
            {
                if (e.SqlState == PostgresErrorCodes.ExclusionViolation) conflicts++; else deadlocks++;
                if (attempt == MaxAttempts)
                {
                    var reason = e.SqlState == PostgresErrorCodes.DeadlockDetected
                        ? FailureReason.DeadlockRetriesExhausted : FailureReason.ConflictRetriesExhausted;
                    return new BookingResult(Outcome.Failed, reason, attempt, conflicts, deadlocks);
                }
                await Task.Delay(Random.Shared.Next(1, 5) * attempt, ct);   // jittered backoff
            }
            catch (OperationCanceledException)
            {
                return new BookingResult(Outcome.Unfinished, DbTransactions: attempt,
                    ConflictRetries: conflicts, DeadlockRetries: deadlocks);
            }
            catch (NpgsqlException e) when (e.InnerException is TimeoutException)
            {
                return new BookingResult(Outcome.Failed, FailureReason.PoolTimeout, attempt, conflicts, deadlocks);
            }
            catch (Exception)
            {
                return new BookingResult(Outcome.Failed, FailureReason.Other, attempt, conflicts, deadlocks);
            }
        }
    }

    private async Task<bool> TryOnceAsync(BookingRequest r, CancellationToken ct)
    {
        await using var conn = await db.OpenConnectionAsync(ct);
        using var _ = gauge.Enter();

        if (!lockDealershipDay)
            return await InsertAsync(conn, null, r, ct);

        await using var tx = await conn.BeginTransactionAsync(ct);
        await using (var lockCmd = new NpgsqlCommand("SELECT pg_advisory_xact_lock($1, $2)", conn, tx))
        {
            lockCmd.Parameters.Add(new() { Value = r.DealershipId });
            lockCmd.Parameters.Add(new() { Value = r.Day });
            await lockCmd.ExecuteNonQueryAsync(ct);
        }
        var booked = await InsertAsync(conn, tx, r, ct);
        await tx.CommitAsync(ct);
        return booked;
    }

    private static async Task<bool> InsertAsync(NpgsqlConnection conn, NpgsqlTransaction? tx, BookingRequest r,
        CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(BookSql, conn, tx);
        cmd.Parameters.Add(new() { Value = r.DealershipId });
        cmd.Parameters.Add(new() { Value = r.Start });
        cmd.Parameters.Add(new() { Value = r.End });
        return await cmd.ExecuteScalarAsync(ct) is not null;
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
