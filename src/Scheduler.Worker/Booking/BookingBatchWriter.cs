using Npgsql;
using NpgsqlTypes;
using Scheduler.Domain;
using Scheduler.Infrastructure;
using Scheduler.Infrastructure.Messaging;

namespace Scheduler.Worker.Booking;

/// <summary>
/// The statements of one batch transaction. Each is a single round trip over arrays (<c>unnest</c>), so a
/// batch costs the same handful of statements whether it holds 1 command or 64.
/// </summary>
internal static class BookingBatchWriter
{
    public static async Task WriteAsync(NpgsqlConnection conn, NpgsqlTransaction tx,
        IReadOnlyList<(CommandEnvelope Envelope, BookingDecision Decision)> decided, IReadOnlyList<Guid> duplicates,
        string eventsTopic, DateTime now, CancellationToken ct)
    {
        if (decided.Count > 0) await InsertBookingRequestsAsync(conn, tx, decided, now, ct);

        var confirmed = decided.Where(d => d.Decision.IsConfirmed).ToArray();
        if (confirmed.Length > 0)
        {
            var customerIds = await UpsertCustomersAsync(conn, tx, confirmed, ct);
            var vehicleIds = await UpsertVehiclesAsync(conn, tx, confirmed, customerIds, ct);
            var appointmentIds = await InsertAppointmentsAsync(conn, tx, confirmed, customerIds, vehicleIds, ct);
            await InsertOutboxAsync(conn, tx, confirmed, appointmentIds, eventsTopic, now, ct);
        }

        // Delivered to listeners only when the transaction commits. Duplicates are included so an API instance
        // waiting on a retried request still wakes up and reads the stored outcome.
        var ids = decided.Select(d => d.Envelope.Command.RequestId).Concat(duplicates).ToArray();
        if (ids.Length > 0) await NotifyAsync(conn, tx, ids, outboxPending: confirmed.Length > 0, ct);
    }

    private static async Task InsertBookingRequestsAsync(NpgsqlConnection conn, NpgsqlTransaction tx,
        IReadOnlyList<(CommandEnvelope Envelope, BookingDecision Decision)> decided, DateTime now, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand("""
            INSERT INTO booking_request (id, dealership_id, service_type_code, requested_start, outcome, received_at, decided_at)
            SELECT id, d, s, st, o, r, $7
              FROM unnest($1::uuid[], $2::int[], $3::text[], $4::timestamptz[], $5::text[], $6::timestamptz[])
                   AS x(id, d, s, st, o, r)
            """, conn, tx);
        cmd.Parameters.Add(new() { Value = decided.Select(d => d.Envelope.Command.RequestId).ToArray() });
        cmd.Parameters.Add(new() { Value = decided.Select(d => d.Envelope.Command.DealershipId).ToArray() });
        cmd.Parameters.Add(new() { Value = decided.Select(d => d.Envelope.Command.ServiceTypeCode).ToArray() });
        cmd.Parameters.Add(new() { Value = decided.Select(d => d.Envelope.Command.Start).ToArray() });
        cmd.Parameters.Add(new() { Value = decided.Select(d => d.Decision.Outcome.ToString()).ToArray() });
        cmd.Parameters.Add(new() { Value = decided.Select(d => d.Envelope.EnqueuedAt).ToArray() });
        cmd.Parameters.Add(new() { Value = now });
        await cmd.ExecuteNonQueryAsync(ct);
    }

    /// <summary>Customers are matched by email. Deduplicated first: ON CONFLICT can't touch one row twice.</summary>
    private static async Task<Dictionary<string, long>> UpsertCustomersAsync(NpgsqlConnection conn, NpgsqlTransaction tx,
        IReadOnlyList<(CommandEnvelope Envelope, BookingDecision Decision)> confirmed, CancellationToken ct)
    {
        var latest = confirmed.Select(c => c.Envelope.Command.Customer)
            .GroupBy(c => c.Email, StringComparer.Ordinal).Select(g => g.Last()).ToArray();
        await using var cmd = new NpgsqlCommand("""
            INSERT INTO customer (email, name)
            SELECT * FROM unnest($1::text[], $2::text[])
            ON CONFLICT (email) DO UPDATE SET name = EXCLUDED.name
            RETURNING email, id
            """, conn, tx);
        cmd.Parameters.Add(new() { Value = latest.Select(c => c.Email).ToArray() });
        cmd.Parameters.Add(new() { Value = latest.Select(c => c.Name).ToArray() });
        return await ReadKeyedIdsAsync(cmd, ct);
    }

    /// <summary>Vehicles are matched by VIN and point at the customer who booked them most recently.</summary>
    private static async Task<Dictionary<string, long>> UpsertVehiclesAsync(NpgsqlConnection conn, NpgsqlTransaction tx,
        IReadOnlyList<(CommandEnvelope Envelope, BookingDecision Decision)> confirmed,
        Dictionary<string, long> customerIds, CancellationToken ct)
    {
        var latest = confirmed.Select(c => c.Envelope.Command)
            .GroupBy(c => c.Vehicle.Vin, StringComparer.Ordinal).Select(g => g.Last()).ToArray();
        await using var cmd = new NpgsqlCommand("""
            INSERT INTO vehicle (vin, make, model, customer_id)
            SELECT * FROM unnest($1::text[], $2::text[], $3::text[], $4::bigint[])
            ON CONFLICT (vin) DO UPDATE SET make = EXCLUDED.make, model = EXCLUDED.model, customer_id = EXCLUDED.customer_id
            RETURNING vin, id
            """, conn, tx);
        cmd.Parameters.Add(new() { Value = latest.Select(c => c.Vehicle.Vin).ToArray() });
        cmd.Parameters.Add(new() { Value = latest.Select(c => c.Vehicle.Make).ToArray() });
        cmd.Parameters.Add(new() { Value = latest.Select(c => c.Vehicle.Model).ToArray() });
        cmd.Parameters.Add(new() { Value = latest.Select(c => customerIds[c.Customer.Email]).ToArray() });
        return await ReadKeyedIdsAsync(cmd, ct);
    }

    private static async Task<Dictionary<Guid, long>> InsertAppointmentsAsync(NpgsqlConnection conn, NpgsqlTransaction tx,
        IReadOnlyList<(CommandEnvelope Envelope, BookingDecision Decision)> confirmed,
        Dictionary<string, long> customerIds, Dictionary<string, long> vehicleIds, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand("""
            INSERT INTO appointment (booking_request_id, dealership_id, customer_id, vehicle_id, service_type_code,
                                     technician_id, bay_id, slot)
            SELECT r, d, c, v, s, t, b, tstzrange(st, en, '[)')
              FROM unnest($1::uuid[], $2::int[], $3::bigint[], $4::bigint[], $5::text[], $6::int[], $7::int[],
                          $8::timestamptz[], $9::timestamptz[]) AS x(r, d, c, v, s, t, b, st, en)
            RETURNING booking_request_id, id
            """, conn, tx);
        var commands = confirmed.Select(c => c.Envelope.Command).ToArray();
        cmd.Parameters.Add(new() { Value = commands.Select(c => c.RequestId).ToArray() });
        cmd.Parameters.Add(new() { Value = commands.Select(c => c.DealershipId).ToArray() });
        cmd.Parameters.Add(new() { Value = commands.Select(c => customerIds[c.Customer.Email]).ToArray() });
        cmd.Parameters.Add(new() { Value = commands.Select(c => vehicleIds[c.Vehicle.Vin]).ToArray() });
        cmd.Parameters.Add(new() { Value = commands.Select(c => c.ServiceTypeCode).ToArray() });
        cmd.Parameters.Add(new() { Value = confirmed.Select(c => c.Decision.TechnicianId!.Value).ToArray() });
        cmd.Parameters.Add(new() { Value = confirmed.Select(c => c.Decision.BayId!.Value).ToArray() });
        cmd.Parameters.Add(new() { Value = confirmed.Select(c => c.Decision.Slot!.Value.Start).ToArray() });
        cmd.Parameters.Add(new() { Value = confirmed.Select(c => c.Decision.Slot!.Value.End).ToArray() });

        var ids = new Dictionary<Guid, long>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct)) ids[reader.GetGuid(0)] = reader.GetInt64(1);
        return ids;
    }

    private static async Task InsertOutboxAsync(NpgsqlConnection conn, NpgsqlTransaction tx,
        IReadOnlyList<(CommandEnvelope Envelope, BookingDecision Decision)> confirmed,
        Dictionary<Guid, long> appointmentIds, string topic, DateTime now, CancellationToken ct)
    {
        var events = confirmed.Select(c => new AppointmentConfirmedEvent(
            appointmentIds[c.Envelope.Command.RequestId], c.Envelope.Command.RequestId, c.Envelope.Command.DealershipId,
            c.Envelope.Command.ServiceTypeCode, c.Decision.TechnicianId!.Value, c.Decision.BayId!.Value,
            c.Decision.Slot!.Value.Start, c.Decision.Slot!.Value.End, now)).ToArray();

        await using var cmd = new NpgsqlCommand("""
            INSERT INTO outbox_message (topic, key, type, payload)
            SELECT $1, k, $2, p FROM unnest($3::text[], $4::jsonb[]) AS x(k, p)
            """, conn, tx);
        cmd.Parameters.Add(new() { Value = topic });
        cmd.Parameters.Add(new() { Value = MessageJson.AppointmentConfirmed });
        cmd.Parameters.Add(new() { Value = events.Select(e => e.DealershipId.ToString()).ToArray() });
        cmd.Parameters.Add(new()
        {
            Value = events.Select(MessageJson.SerializeToString).ToArray(),
            NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Jsonb,
        });
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private static async Task NotifyAsync(NpgsqlConnection conn, NpgsqlTransaction tx, Guid[] ids, bool outboxPending, CancellationToken ct)
    {
        // NOTIFY payloads are capped at 8000 bytes; a 36-character id plus a comma fits ~200 per message.
        foreach (var chunk in ids.Chunk(150))
        {
            await using var cmd = new NpgsqlCommand("SELECT pg_notify($1, $2)", conn, tx);
            cmd.Parameters.Add(new() { Value = PgChannels.BookingDecided });
            cmd.Parameters.Add(new() { Value = string.Join(',', chunk) });
            await cmd.ExecuteNonQueryAsync(ct);
        }
        if (outboxPending)
        {
            await using var cmd = new NpgsqlCommand($"SELECT pg_notify('{PgChannels.OutboxPending}', '')", conn, tx);
            await cmd.ExecuteNonQueryAsync(ct);
        }
    }

    private static async Task<Dictionary<string, long>> ReadKeyedIdsAsync(NpgsqlCommand cmd, CancellationToken ct)
    {
        var ids = new Dictionary<string, long>(StringComparer.Ordinal);
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct)) ids[reader.GetString(0)] = reader.GetInt64(1);
        return ids;
    }
}
