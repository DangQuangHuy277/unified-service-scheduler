using Microsoft.AspNetCore.Mvc;
using Scheduler.Api.Booking;
using Scheduler.Infrastructure.Data;

namespace Scheduler.Api.Controllers;

[ApiController]
[Route("booking-requests")]
[Produces("application/json")]
public sealed class BookingRequestsController(BookingReadQueries queries, TimeProvider clock) : ControllerBase
{
    /// <summary>An id never seen after this long is unknown, not pending (UUIDv7 ids only).</summary>
    private static readonly TimeSpan PendingHorizon = TimeSpan.FromMinutes(5);

    /// <summary>Status of a booking request: pending (202), confirmed or rejected (200).</summary>
    [HttpGet("{id:guid}")]
    [EndpointSummary("Get a booking request's status")]
    [ProducesResponseType<BookingRequestStatus>(StatusCodes.Status200OK)]
    [ProducesResponseType<BookingRequestStatus>(StatusCodes.Status202Accepted)]
    [ProducesResponseType<ProblemDetails>(StatusCodes.Status404NotFound, "application/problem+json")]
    public async Task<IActionResult> Get(Guid id, CancellationToken ct)
    {
        var decision = await queries.FindDecisionAsync(id, ct);
        if (decision is not null)
        {
            return Ok(new BookingRequestStatus(id, decision.AppointmentId is null ? "rejected" : "confirmed",
                decision.Outcome.ToString(), decision.AppointmentId, new DateTimeOffset(decision.DecidedAt, TimeSpan.Zero)));
        }
        if (RequestIds.CreatedAt(id) is { } created && clock.GetUtcNow() - created > PendingHorizon)
            return Problem(statusCode: StatusCodes.Status404NotFound, title: "Unknown booking request");
        return Accepted($"/booking-requests/{id}", BookingRequestStatus.Pending(id));
    }
}
