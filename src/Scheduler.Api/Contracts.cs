using System.ComponentModel.DataAnnotations;

namespace Scheduler.Api;

// Request models are validated by [ApiController]: a failure returns 400 with a ValidationProblem body
// before any action runs. Business rules (opening hours, capacity) are checked later and answer 409/422.
// They use init-only properties rather than positional records because MVC then validates them as
// properties, so error keys use the JSON names clients send ("customer.email"); positional record
// parameters keep their C# casing ("Customer.Email").

public sealed record BookAppointmentRequest
{
    [Range(1, int.MaxValue, ErrorMessage = "Required, a positive id.")]
    public int DealershipId { get; init; }

    [Required(ErrorMessage = "Required, e.g. \"OIL\".")]
    public string? ServiceType { get; init; }

    [Required(ErrorMessage = "Required, ISO 8601 with an offset, e.g. 2027-03-04T09:00:00+00:00.")]
    public DateTimeOffset? Start { get; init; }

    [Required]
    public CustomerInput? Customer { get; init; }

    [Required]
    public VehicleInput? Vehicle { get; init; }
}

public sealed record CustomerInput
{
    [Required, StringLength(200)]
    public string? Name { get; init; }

    [Required, EmailAddress, StringLength(320)]
    public string? Email { get; init; }
}

public sealed record VehicleInput
{
    [Required, RegularExpression("^[A-HJ-NPR-Za-hj-npr-z0-9]{17}$", ErrorMessage = "17 characters, letters and digits except I, O and Q.")]
    public string? Vin { get; init; }

    [Required, StringLength(100)]
    public string? Make { get; init; }

    [Required, StringLength(100)]
    public string? Model { get; init; }
}

public sealed record AvailabilityQuery
{
    [Required]
    public int? DealershipId { get; init; }

    [Required]
    public string? ServiceType { get; init; }

    [Required(ErrorMessage = "Required, as YYYY-MM-DD in the dealership's local calendar.")]
    public DateOnly? Date { get; init; }
}

/// <summary>Times are given in the dealership's local offset, so "09:00+01:00" reads as the customer sees it.</summary>
public sealed record AppointmentResponse(
    long Id,
    Guid RequestId,
    NamedRef<int> Dealership,
    NamedRef<string> Service,
    DateTimeOffset Start,
    DateTimeOffset End,
    NamedRef<int> Technician,
    NamedRef<int> Bay,
    CustomerResponse Customer,
    VehicleResponse Vehicle,
    DateTimeOffset CreatedAt);

public sealed record NamedRef<TId>(TId Id, string Name);

public sealed record CustomerResponse(long Id, string Name, string Email);

public sealed record VehicleResponse(long Id, string Vin, string Make, string Model);

public sealed record BookingRequestStatus(
    Guid RequestId,
    string Status,
    string? Outcome,
    long? AppointmentId,
    DateTimeOffset? DecidedAt)
{
    public static BookingRequestStatus Pending(Guid requestId) => new(requestId, "pending", null, null, null);
}

public sealed record AvailabilityResponse(
    int DealershipId,
    string ServiceType,
    DateOnly Date,
    string TimeZone,
    DateTimeOffset GeneratedAt,
    IReadOnlyList<AvailableSlot> Slots);

public sealed record AvailableSlot(DateTimeOffset Start, DateTimeOffset End);
