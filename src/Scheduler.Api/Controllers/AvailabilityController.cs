using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;
using Scheduler.Api.Booking;
using Scheduler.Domain;
using Scheduler.Infrastructure;
using Scheduler.Infrastructure.Data;

namespace Scheduler.Api.Controllers;

[ApiController]
[Route("availability")]
[Produces("application/json")]
public sealed class AvailabilityController(
    ReferenceDataCache reference,
    ReadModelStore readModel,
    TimeProvider clock,
    IOptions<BookingOptions> options) : ControllerBase
{
    /// <summary>Start times that currently fit a service on one local day.</summary>
    /// <remarks>
    /// Served from the availability read model, which trails bookings by the projection delay. A slot shown
    /// here can still be taken by the time it is booked; the booking then answers 409.
    /// </remarks>
    [HttpGet]
    [EndpointSummary("Get free start times for a service")]
    [ProducesResponseType<AvailabilityResponse>(StatusCodes.Status200OK)]
    [ProducesResponseType<ValidationProblemDetails>(StatusCodes.Status400BadRequest, "application/problem+json")]
    [ProducesResponseType<ProblemDetails>(StatusCodes.Status422UnprocessableEntity, "application/problem+json")]
    public async Task<IActionResult> Get([FromQuery] AvailabilityQuery query, CancellationToken ct)
    {
        var profile = await reference.FindDealershipAsync(query.DealershipId!.Value, ct);
        if (profile is null) return BookingProblems.Rejected(this, BookingOutcome.UnknownDealership);
        var service = await reference.FindServiceTypeAsync(query.ServiceType!, ct);
        if (service is null) return BookingProblems.Rejected(this, BookingOutcome.UnknownServiceType);

        // The local day's bounds in UTC; a daylight-saving day is 23 or 25 hours long.
        var date = query.Date!.Value;
        var dayStartUtc = TimeZoneInfo.ConvertTimeToUtc(date.ToDateTime(TimeOnly.MinValue), profile.TimeZone);
        var dayEndUtc = TimeZoneInfo.ConvertTimeToUtc(date.AddDays(1).ToDateTime(TimeOnly.MinValue), profile.TimeZone);
        var schedule = new DealershipSchedule(profile);
        foreach (var (technicianId, bayId, busySlot) in await readModel.BusyAsync(profile.Id, dayStartUtc, dayEndUtc, ct))
            schedule.Add(technicianId, bayId, busySlot);

        var now = clock.GetUtcNow();
        var free = schedule.FreeSlots(service, date, now.UtcDateTime, options.Value.AvailabilityStep);
        return Ok(new AvailabilityResponse(profile.Id, service.Code, date, profile.TimeZone.Id, now,
            free.Select(s => new AvailableSlot(AppointmentMapping.Local(s.Start, profile.TimeZone), AppointmentMapping.Local(s.End, profile.TimeZone))).ToArray()));
    }
}
