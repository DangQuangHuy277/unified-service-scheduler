using NpgsqlTypes;

namespace Scheduler.Infrastructure.Persistence;

// Storage shapes. The domain types in Scheduler.Domain stay free of persistence concerns;
// ReferenceDataStore maps these rows into them.

public sealed class DealershipRow
{
    public int Id { get; set; }
    public required string Name { get; set; }

    /// <summary>IANA zone id, e.g. "Europe/London" (A6).</summary>
    public required string TimeZone { get; set; }

    public List<OpeningHoursRow> OpeningHours { get; set; } = [];
    public List<TechnicianRow> Technicians { get; set; } = [];
    public List<BayRow> Bays { get; set; } = [];
}

public sealed class OpeningHoursRow
{
    public int DealershipId { get; set; }
    public DayOfWeek Weekday { get; set; }
    public TimeOnly Opens { get; set; }
    public TimeOnly Closes { get; set; }
}

public sealed class ServiceTypeRow
{
    public required string Code { get; set; }
    public required string Name { get; set; }
    public int DurationMinutes { get; set; }
    public required string RequiredSkill { get; set; }
}

public sealed class TechnicianRow
{
    public int Id { get; set; }
    public int DealershipId { get; set; }
    public required string Name { get; set; }
    public string[] Skills { get; set; } = [];
}

public sealed class BayRow
{
    public int Id { get; set; }
    public int DealershipId { get; set; }
    public required string Name { get; set; }
}

public sealed class CustomerRow
{
    public long Id { get; set; }
    public required string Email { get; set; }
    public required string Name { get; set; }
}

public sealed class VehicleRow
{
    public long Id { get; set; }
    public required string Vin { get; set; }
    public required string Make { get; set; }
    public required string Model { get; set; }
    public long CustomerId { get; set; }
}

/// <summary>
/// One row per <em>decided</em> request, written by the owning worker in the same transaction as the
/// appointment. A request with no row is still pending (the API never writes here, see D-005).
/// </summary>
public sealed class BookingRequestRow
{
    public Guid Id { get; set; }
    public int DealershipId { get; set; }
    public required string ServiceTypeCode { get; set; }
    public DateTime RequestedStart { get; set; }

    /// <summary>A <see cref="Scheduler.Domain.BookingOutcome"/> name.</summary>
    public required string Outcome { get; set; }

    public DateTime ReceivedAt { get; set; }
    public DateTime DecidedAt { get; set; }
}

public sealed class AppointmentRow
{
    public long Id { get; set; }
    public Guid BookingRequestId { get; set; }
    public int DealershipId { get; set; }
    public long CustomerId { get; set; }
    public long VehicleId { get; set; }
    public required string ServiceTypeCode { get; set; }
    public int TechnicianId { get; set; }
    public int BayId { get; set; }

    /// <summary>Half-open <c>[start, end)</c>; guarded by the two exclusion constraints.</summary>
    public NpgsqlRange<DateTime> Slot { get; set; }

    public DateTime CreatedAt { get; set; }
}

/// <summary>Transactional outbox: events written with the booking, published by OutboxRelay.</summary>
public sealed class OutboxMessageRow
{
    public long Id { get; set; }
    public required string Topic { get; set; }
    public required string Key { get; set; }
    public required string Type { get; set; }
    public required string Payload { get; set; }
    public DateTime CreatedAt { get; set; }
    public DateTime? PublishedAt { get; set; }
}

/// <summary>
/// Fencing token per Kafka partition. A worker increments the epoch when it is assigned the partition,
/// and every batch it writes checks the epoch is still its own (D-007).
/// </summary>
public sealed class PartitionLeaseRow
{
    public required string Topic { get; set; }
    public int Partition { get; set; }
    public long Epoch { get; set; }
    public required string Owner { get; set; }
    public DateTime AcquiredAt { get; set; }
}

/// <summary>Read model (schema "read"), filled asynchronously from AppointmentConfirmed events.</summary>
public sealed class AvailabilityBusyRow
{
    public long AppointmentId { get; set; }
    public int DealershipId { get; set; }
    public int TechnicianId { get; set; }
    public int BayId { get; set; }
    public NpgsqlRange<DateTime> Slot { get; set; }
    public DateTime ConfirmedAt { get; set; }
    public DateTime ProjectedAt { get; set; }
}
