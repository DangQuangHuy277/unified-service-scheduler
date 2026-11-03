using Npgsql;
using Scheduler.Domain;

namespace Scheduler.Infrastructure.Data;

public sealed record StoredDecision(BookingOutcome Outcome, DateTime DecidedAt, long? AppointmentId);

public sealed record NamedId<TId>(TId Id, string Name);

/// <summary>An appointment with everything it links to (R3). Times are UTC; <see cref="TimeZone"/> is the dealership's.</summary>
public sealed record AppointmentDetails(
    long Id,
    Guid RequestId,
    NamedId<int> Dealership,
    string TimeZone,
    NamedId<string> Service,
    DateTime StartUtc,
    DateTime EndUtc,
    NamedId<int> Technician,
    NamedId<int> Bay,
    long CustomerId,
    string CustomerName,
    string CustomerEmail,
    long VehicleId,
    string Vin,
    string Make,
    string Model,
    DateTime CreatedAtUtc);

/// <summary>The API's reads of the write model. Plain SQL: each lookup is one round trip.</summary>
public sealed class BookingReadQueries(NpgsqlDataSource db)
{
    /// <summary>The stored outcome of a request, or null while it is still pending.</summary>
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

    public async Task<AppointmentDetails?> FindAppointmentAsync(long id, CancellationToken ct)
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
        return new AppointmentDetails(
            r.GetInt64(0), r.GetGuid(1),
            new NamedId<int>(r.GetInt32(5), r.GetString(6)), r.GetString(7),
            new NamedId<string>(r.GetString(8), r.GetString(9)),
            r.GetDateTime(2), r.GetDateTime(3),
            new NamedId<int>(r.GetInt32(10), r.GetString(11)),
            new NamedId<int>(r.GetInt32(12), r.GetString(13)),
            r.GetInt64(14), r.GetString(15), r.GetString(16),
            r.GetInt64(17), r.GetString(18), r.GetString(19), r.GetString(20),
            r.GetDateTime(4));
    }
}
