namespace Scheduler.Domain;

/// <summary>
/// One dealership's busy technicians and bays. In the booking worker exactly one instance exists per
/// dealership, owned by the worker that owns its Kafka partition, so <see cref="TryReserve"/> never races
/// and needs no locks. The availability endpoint builds a throwaway instance from the read model and only
/// calls <see cref="FreeSlots"/>.
/// </summary>
public sealed class DealershipSchedule
{
    private readonly Dictionary<(int Id, DateOnly LocalDay), List<TimeSlot>> _technicianBusy = new();
    private readonly Dictionary<(int Id, DateOnly LocalDay), List<TimeSlot>> _bayBusy = new();
    private readonly IReadOnlyList<Technician> _technicians;
    private readonly IReadOnlyList<Bay> _bays;

    public DealershipSchedule(DealershipProfile profile)
    {
        Profile = profile;
        // First-fit by id (A9): deterministic, so tests and replays make the same choices.
        _technicians = profile.Technicians.OrderBy(t => t.Id).ToArray();
        _bays = profile.Bays.OrderBy(b => b.Id).ToArray();
    }

    public DealershipProfile Profile { get; }

    /// <summary>Records an existing appointment, for example when loading the schedule from the database.</summary>
    public void Add(int technicianId, int bayId, TimeSlot slot)
    {
        foreach (var day in LocalDays(slot))
        {
            Busy(_technicianBusy, technicianId, day).Add(slot);
            Busy(_bayBusy, bayId, day).Add(slot);
        }
    }

    /// <summary>
    /// Decides one request: validates it, then reserves the first free qualified technician and the first
    /// free bay for the whole slot (R2). A confirmed decision is already applied to this schedule.
    /// </summary>
    public BookingDecision TryReserve(ServiceType service, DateTime startUtc, DateTime nowUtc)
    {
        var slot = TimeSlot.StartingAt(startUtc, service.Duration);
        if (slot.Start < nowUtc) return BookingDecision.Rejected(BookingOutcome.InThePast, slot);
        if (!Profile.Hours.Contains(slot, Profile.TimeZone))
            return BookingDecision.Rejected(BookingOutcome.OutsideOpeningHours, slot);
        if (!_technicians.Any(t => t.IsQualifiedFor(service)))
            return BookingDecision.Rejected(BookingOutcome.NoQualifiedTechnician, slot);

        var technician = FirstFreeTechnician(service, slot);
        var bay = FirstFreeBay(slot);
        if (technician is null || bay is null) return BookingDecision.Rejected(BookingOutcome.NoCapacity, slot);

        Add(technician.Id, bay.Id, slot);
        return BookingDecision.Confirmed(slot, technician.Id, bay.Id);
    }

    /// <summary>
    /// Start times on one local day where the service would currently be confirmed. Candidates are spaced
    /// by <paramref name="step"/> from opening time; nothing is reserved.
    /// </summary>
    public IReadOnlyList<TimeSlot> FreeSlots(ServiceType service, DateOnly localDay, DateTime nowUtc, TimeSpan step)
    {
        if (step <= TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(step));
        if (!Profile.Hours.Days.TryGetValue(localDay.DayOfWeek, out var hours)) return [];

        var free = new List<TimeSlot>();
        var localStart = localDay.ToDateTime(hours.Opens);
        var localClose = localDay.ToDateTime(hours.Closes);
        for (var local = localStart; local + service.Duration <= localClose; local += step)
        {
            if (Profile.TimeZone.IsInvalidTime(local)) continue;   // skipped by a daylight-saving jump
            var startUtc = TimeZoneInfo.ConvertTimeToUtc(local, Profile.TimeZone);
            var slot = TimeSlot.StartingAt(startUtc, service.Duration);
            if (slot.Start < nowUtc || !Profile.Hours.Contains(slot, Profile.TimeZone)) continue;
            if (FirstFreeTechnician(service, slot) is not null && FirstFreeBay(slot) is not null) free.Add(slot);
        }
        return free;
    }

    private Technician? FirstFreeTechnician(ServiceType service, TimeSlot slot) =>
        _technicians.FirstOrDefault(t => t.IsQualifiedFor(service) && IsFree(_technicianBusy, t.Id, slot));

    private Bay? FirstFreeBay(TimeSlot slot) => _bays.FirstOrDefault(b => IsFree(_bayBusy, b.Id, slot));

    private bool IsFree(Dictionary<(int, DateOnly), List<TimeSlot>> busy, int id, TimeSlot slot)
    {
        foreach (var day in LocalDays(slot))
        {
            if (!busy.TryGetValue((id, day), out var taken)) continue;
            foreach (var other in taken)
                if (other.Overlaps(slot)) return false;
        }
        return true;
    }

    /// <summary>Local calendar days the slot touches; the index key, so a lookup scans one day's jobs.</summary>
    private IEnumerable<DateOnly> LocalDays(TimeSlot slot)
    {
        var first = DateOnly.FromDateTime(TimeZoneInfo.ConvertTimeFromUtc(slot.Start, Profile.TimeZone));
        var last = DateOnly.FromDateTime(TimeZoneInfo.ConvertTimeFromUtc(slot.End.AddTicks(-1), Profile.TimeZone));
        for (var day = first; day <= last; day = day.AddDays(1)) yield return day;
    }

    private static List<TimeSlot> Busy(Dictionary<(int, DateOnly), List<TimeSlot>> busy, int id, DateOnly day)
    {
        if (!busy.TryGetValue((id, day), out var list)) busy[(id, day)] = list = [];
        return list;
    }
}
