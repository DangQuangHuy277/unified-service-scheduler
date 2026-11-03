using Npgsql;
using Scheduler.Domain;

namespace Scheduler.Api.Booking;

public sealed record StoredDecision(BookingOutcome Outcome, DateTime DecidedAt, long? AppointmentId);

/// <summary>Read paths. Plain SQL: each endpoint is one round trip.</summary>
public sealed class BookingQueries(NpgsqlDataSource db)
{
    public async Task<StoredDecision?> FindDecisionAsync(Guid requestId, CancellationToken ct)
    {
        await using var cmd = db.CreateCommand("""
            SELECT r.outcome, r.decided_at, a.id
              FROM booking_request r
              LEFT JOIN appointment a ON a.booking_request_id = r.id
             WHERE r.id = $1
            """);
        cmd.Parameters.Add(new() { Value = requestId });
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        if (!await reader.ReadAsync(ct)) return null;
        return new StoredDecision(
            Enum.Parse<BookingOutcome>(reader.GetString(0)),
            reader.GetDateTime(1),
            reader.IsDBNull(2) ? null : reader.GetInt64(2));
    }

    public async Task<AppointmentResponse?> FindAppointmentAsync(long id, CancellationToken ct)
    {
        await using var cmd = db.CreateCommand("""
            SELECT a.id, a.booking_request_id, lower(a.slot), upper(a.slot), a.created_at,
                   d.id, d.name, d.time_zone,
                   s.code, s.name,
                   t.id, t.name,
                   b.id, b.name,
                   c.id, c.name, c.email,
                   v.id, v.vin, v.make, v.model
              FROM appointment a
              JOIN dealership d   ON d.id = a.dealership_id
              JOIN service_type s ON s.code = a.service_type_code
              JOIN technician t   ON t.id = a.technician_id
              JOIN bay b          ON b.id = a.bay_id
              JOIN customer c     ON c.id = a.customer_id
              JOIN vehicle v      ON v.id = a.vehicle_id
             WHERE a.id = $1
            """);
        cmd.Parameters.Add(new() { Value = id });
        await using var r = await cmd.ExecuteReaderAsync(ct);
        if (!await r.ReadAsync(ct)) return null;

        var zone = TimeZoneInfo.FindSystemTimeZoneById(r.GetString(7));
        return new AppointmentResponse(
            r.GetInt64(0),
            r.GetGuid(1),
            new NamedRef<int>(r.GetInt32(5), r.GetString(6)),
            new NamedRef<string>(r.GetString(8), r.GetString(9)),
            Local(r.GetDateTime(2), zone),
            Local(r.GetDateTime(3), zone),
            new NamedRef<int>(r.GetInt32(10), r.GetString(11)),
            new NamedRef<int>(r.GetInt32(12), r.GetString(13)),
            new CustomerResponse(r.GetInt64(14), r.GetString(15), r.GetString(16)),
            new VehicleResponse(r.GetInt64(17), r.GetString(18), r.GetString(19), r.GetString(20)),
            new DateTimeOffset(r.GetDateTime(4), TimeSpan.Zero));
    }

    /// <summary>Busy technicians and bays from the read model, which lags the write side by the projection delay.</summary>
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

    public static DateTimeOffset Local(DateTime utc, TimeZoneInfo zone) =>
        TimeZoneInfo.ConvertTime(new DateTimeOffset(utc, TimeSpan.Zero), zone);
}
