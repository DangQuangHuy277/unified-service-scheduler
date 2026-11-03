using Npgsql;
using Scheduler.Domain;
using Scheduler.Infrastructure.Messaging;

namespace Scheduler.Infrastructure.Data;

/// <summary>
/// The availability read model, <c>read.availability_busy</c>: written by the projector (Worker), read by
/// <c>GET /availability</c> (Api). It trails the write side by the projection delay.
/// </summary>
public sealed class ReadModelStore(NpgsqlDataSource db)
{
    /// <summary>Idempotent by appointment id, so redelivered or re-relayed events are harmless.</summary>
    public async Task UpsertBusyAsync(IReadOnlyList<AppointmentConfirmedEvent> events, CancellationToken ct)
    {
        await using var cmd = db.CreateCommand("""
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
    }

    /// <summary>Busy technicians and bays for one dealership between two instants.</summary>
    public async Task<IReadOnlyList<(int TechnicianId, int BayId, TimeSlot Slot)>> BusyAsync(
        int dealershipId, DateTime fromUtc, DateTime toUtc, CancellationToken ct)
    {
        await using var cmd = db.CreateCommand("""
            SELECT technician_id, bay_id, lower(slot), upper(slot)
              FROM read.availability_busy
             WHERE dealership_id = $1 AND slot && tstzrange($2, $3)
            """);
        cmd.Parameters.Add(new() { Value = dealershipId });
        cmd.Parameters.Add(new() { Value = fromUtc });
        cmd.Parameters.Add(new() { Value = toUtc });
        var busy = new List<(int, int, TimeSlot)>();
        await using var r = await cmd.ExecuteReaderAsync(ct);
        while (await r.ReadAsync(ct))
            busy.Add((r.GetInt32(0), r.GetInt32(1), new TimeSlot(r.GetDateTime(2), r.GetDateTime(3))));
        return busy;
    }
}
