using Scheduler.Infrastructure.Data;

namespace Scheduler.Api.Booking;

/// <summary>
/// From the data layer's shapes (UTC, plain records) to the public API contract (dealership-local offsets).
/// Keeping this here means the data layer never depends on HTTP contracts.
/// </summary>
public static class AppointmentMapping
{
    public static AppointmentResponse ToResponse(this AppointmentDetails a)
    {
        var zone = TimeZoneInfo.FindSystemTimeZoneById(a.TimeZone);
        return new AppointmentResponse(
            a.Id,
            a.RequestId,
            new NamedRef<int>(a.Dealership.Id, a.Dealership.Name),
            new NamedRef<string>(a.Service.Id, a.Service.Name),
            Local(a.StartUtc, zone),
            Local(a.EndUtc, zone),
            new NamedRef<int>(a.Technician.Id, a.Technician.Name),
            new NamedRef<int>(a.Bay.Id, a.Bay.Name),
            new CustomerResponse(a.CustomerId, a.CustomerName, a.CustomerEmail),
            new VehicleResponse(a.VehicleId, a.Vin, a.Make, a.Model),
            new DateTimeOffset(a.CreatedAtUtc, TimeSpan.Zero));
    }

    /// <summary>A UTC instant shown in the dealership's local offset, e.g. 09:00+01:00 in London in summer.</summary>
    public static DateTimeOffset Local(DateTime utc, TimeZoneInfo zone) =>
        TimeZoneInfo.ConvertTime(new DateTimeOffset(utc, TimeSpan.Zero), zone);
}
