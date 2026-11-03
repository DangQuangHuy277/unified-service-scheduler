using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Infrastructure;
using Scheduler.Domain;

namespace Scheduler.Api.Controllers;

/// <summary>
/// RFC 9457 problem responses for rejected bookings. 409 = the slot is taken (try another time);
/// 422 = the request can never be booked as asked. The <c>outcome</c> extension is machine-readable.
/// </summary>
public static class BookingProblems
{
    public static ObjectResult Rejected(ControllerBase controller, BookingOutcome outcome, Guid? requestId = null)
    {
        var (status, title) = outcome switch
        {
            BookingOutcome.NoCapacity => (StatusCodes.Status409Conflict, "No technician and bay are free for the whole slot"),
            BookingOutcome.NoQualifiedTechnician => (StatusCodes.Status422UnprocessableEntity, "No technician at this dealership is qualified for the service"),
            BookingOutcome.OutsideOpeningHours => (StatusCodes.Status422UnprocessableEntity, "The slot is outside the dealership's opening hours"),
            BookingOutcome.InThePast => (StatusCodes.Status422UnprocessableEntity, "The slot starts in the past"),
            BookingOutcome.UnknownDealership => (StatusCodes.Status422UnprocessableEntity, "Unknown dealership"),
            BookingOutcome.UnknownServiceType => (StatusCodes.Status422UnprocessableEntity, "Unknown service type"),
            _ => throw new ArgumentOutOfRangeException(nameof(outcome), outcome, null),
        };
        var factory = controller.HttpContext.RequestServices.GetRequiredService<ProblemDetailsFactory>();
        var problem = factory.CreateProblemDetails(controller.HttpContext, status, title);
        problem.Extensions["outcome"] = outcome.ToString();
        if (requestId is not null) problem.Extensions["requestId"] = requestId;
        return new ObjectResult(problem) { StatusCode = status, ContentTypes = { "application/problem+json" } };
    }
}
