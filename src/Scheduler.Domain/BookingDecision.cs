namespace Scheduler.Domain;

public enum BookingOutcome
{
    Confirmed,

    /// <summary>Every qualified technician or every bay is busy for part of the slot.</summary>
    NoCapacity,

    /// <summary>The dealership has no technician with the required skill at all.</summary>
    NoQualifiedTechnician,

    OutsideOpeningHours,
    InThePast,
    UnknownDealership,
    UnknownServiceType,
}

/// <summary>The planner's answer for one request. Technician and bay are set only when confirmed.</summary>
public readonly record struct BookingDecision(BookingOutcome Outcome, TimeSlot? Slot, int? TechnicianId, int? BayId)
{
    public bool IsConfirmed => Outcome == BookingOutcome.Confirmed;

    public static BookingDecision Confirmed(TimeSlot slot, int technicianId, int bayId) =>
        new(BookingOutcome.Confirmed, slot, technicianId, bayId);

    public static BookingDecision Rejected(BookingOutcome outcome, TimeSlot? slot = null) =>
        outcome == BookingOutcome.Confirmed
            ? throw new ArgumentException("Use Confirmed() for a confirmed decision.", nameof(outcome))
            : new(outcome, slot, null, null);
}
