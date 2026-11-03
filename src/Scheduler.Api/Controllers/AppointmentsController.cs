using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;
using Scheduler.Api.Booking;
using Scheduler.Domain;
using Scheduler.Infrastructure;
using Scheduler.Infrastructure.Data;
using Scheduler.Infrastructure.Messaging;

namespace Scheduler.Api.Controllers;

[ApiController]
[Route("appointments")]
[Produces("application/json")]
public sealed class AppointmentsController(
    ReferenceDataCache reference,
    BookingCommandPublisher publisher,
    DecisionWaiter waiter,
    BookingReadQueries queries,
    TimeProvider clock,
    IOptions<BookingOptions> options) : ControllerBase
{
    /// <summary>Request an appointment.</summary>
    /// <remarks>
    /// Validates the request, enqueues it for the worker that owns the dealership, and waits up to 2 s
    /// (or <c>Prefer: wait=N</c>, max 10; <c>Prefer: respond-async</c> to not wait) for the decision.
    /// 201 = confirmed, 409 = no capacity, 422 = cannot be booked as asked, 202 = still deciding: poll the Location.
    /// Send <c>Idempotency-Key</c> to make retries safe.
    /// </remarks>
    [HttpPost]
    [EndpointSummary("Request an appointment")]
    [ProducesResponseType<AppointmentResponse>(StatusCodes.Status201Created)]
    [ProducesResponseType<BookingRequestStatus>(StatusCodes.Status202Accepted)]
    [ProducesResponseType<ValidationProblemDetails>(StatusCodes.Status400BadRequest, "application/problem+json")]
    [ProducesResponseType<ProblemDetails>(StatusCodes.Status409Conflict, "application/problem+json")]
    [ProducesResponseType<ProblemDetails>(StatusCodes.Status422UnprocessableEntity, "application/problem+json")]
    public async Task<IActionResult> Book(
        BookAppointmentRequest body,
        [FromHeader(Name = "Idempotency-Key")] string? idempotencyKey,
        [FromHeader(Name = "Prefer")] string? prefer,
        CancellationToken ct)
    {
        // Fail fast on what the cached reference data can already tell; the worker re-checks all of it.
        var now = clock.GetUtcNow().UtcDateTime;
        var profile = await reference.FindDealershipAsync(body.DealershipId, ct);
        if (profile is null) return BookingProblems.Rejected(this, BookingOutcome.UnknownDealership);
        var service = await reference.FindServiceTypeAsync(body.ServiceType!, ct);
        if (service is null) return BookingProblems.Rejected(this, BookingOutcome.UnknownServiceType);
        var slot = TimeSlot.StartingAt(body.Start!.Value.UtcDateTime, service.Duration);
        if (slot.Start < now) return BookingProblems.Rejected(this, BookingOutcome.InThePast);
        if (!profile.Hours.Contains(slot, profile.TimeZone))
            return BookingProblems.Rejected(this, BookingOutcome.OutsideOpeningHours);
        if (!profile.Technicians.Any(t => t.IsQualifiedFor(service)))
            return BookingProblems.Rejected(this, BookingOutcome.NoQualifiedTechnician);

        var requestId = string.IsNullOrWhiteSpace(idempotencyKey)
            ? RequestIds.New()
            : RequestIds.FromIdempotencyKey(idempotencyKey);
        var command = new BookAppointmentCommand(
            requestId, profile.Id, service.Code, slot.Start,
            new CustomerDetails(body.Customer!.Name!.Trim(), body.Customer.Email!.Trim().ToLowerInvariant()),
            new VehicleDetails(body.Vehicle!.Vin!.Trim().ToUpperInvariant(), body.Vehicle.Make!.Trim(),
                body.Vehicle.Model!.Trim()),
            now);

        using var registration = waiter.Register(requestId);
        await publisher.PublishAsync(command);
        var wait = PreferredWait(prefer, options.Value);
        var signalled = await registration.WaitAsync(wait, ct);

        // Read once even without a signal: the notification may have been missed (e.g. the LISTEN connection
        // was reconnecting), or this is a retry of a request that was decided earlier.
        var decision = await queries.FindDecisionAsync(requestId, ct);
        if (decision is null)
        {
            if (wait > TimeSpan.Zero && !signalled) SchedulerTelemetry.ReplyTimeouts.Add(1);
            return Accepted($"/booking-requests/{requestId}", BookingRequestStatus.Pending(requestId));
        }

        if (decision.AppointmentId is { } appointmentId && await queries.FindAppointmentAsync(appointmentId, ct) is
                { } appointment)
            return Created($"/appointments/{appointmentId}", appointment.ToResponse());
        return BookingProblems.Rejected(this, decision.Outcome, requestId);
    }

    /// <summary>A confirmed appointment with its customer, vehicle, technician and bay.</summary>
    [HttpGet("{id:long}")]
    [EndpointSummary("Get an appointment")]
    [ProducesResponseType<AppointmentResponse>(StatusCodes.Status200OK)]
    [ProducesResponseType<ProblemDetails>(StatusCodes.Status404NotFound, "application/problem+json")]
    public async Task<IActionResult> Get(long id, CancellationToken ct) =>
        await queries.FindAppointmentAsync(id, ct) is { } appointment
            ? Ok(appointment.ToResponse())
            : Problem(statusCode: StatusCodes.Status404NotFound, title: "Unknown appointment");

    /// <summary>RFC 7240: <c>Prefer: wait=N</c> (seconds) or <c>Prefer: respond-async</c> (don't wait).</summary>
    private static TimeSpan PreferredWait(string? prefer, BookingOptions options)
    {
        foreach (var token in (prefer ?? "").Split(',', StringSplitOptions.TrimEntries))
        {
            if (token.Equals("respond-async", StringComparison.OrdinalIgnoreCase)) return TimeSpan.Zero;
            if (token.StartsWith("wait=", StringComparison.OrdinalIgnoreCase) &&
                int.TryParse(token[5..], out var seconds) && seconds >= 0)
                return TimeSpan.FromSeconds(Math.Min(seconds, options.MaxReplyWait.TotalSeconds));
        }

        return options.ReplyWait;
    }
}