namespace Scheduler.Domain;

/// <summary>A bookable service. Fixed duration and one required skill (A1, A2).</summary>
public sealed record ServiceType(string Code, string Name, TimeSpan Duration, string RequiredSkill);

/// <summary>A technician is qualified for a service when their skills include its required skill (A2).</summary>
public sealed record Technician(int Id, string Name, IReadOnlySet<string> Skills)
{
    public bool IsQualifiedFor(ServiceType service) => Skills.Contains(service.RequiredSkill);
}

/// <summary>Bays are generic in v1: any bay fits any service (A3).</summary>
public sealed record Bay(int Id, string Name);

/// <summary>Opening times per weekday, in the dealership's local time. A missing day is closed.</summary>
public sealed class OpeningHours(IReadOnlyDictionary<DayOfWeek, (TimeOnly Opens, TimeOnly Closes)> days)
{
    public IReadOnlyDictionary<DayOfWeek, (TimeOnly Opens, TimeOnly Closes)> Days { get; } = days;

    public static OpeningHours EveryDay(TimeOnly opens, TimeOnly closes) =>
        new(Enum.GetValues<DayOfWeek>().ToDictionary(d => d, _ => (opens, closes)));

    /// <summary>
    /// True when the whole slot falls inside one local day's opening hours (A5). The slot is converted
    /// to local time first, so daylight-saving changes are handled by the time zone rules (A6).
    /// </summary>
    public bool Contains(TimeSlot slot, TimeZoneInfo zone)
    {
        var start = TimeZoneInfo.ConvertTimeFromUtc(slot.Start, zone);
        var end = TimeZoneInfo.ConvertTimeFromUtc(slot.End, zone);
        if (start.Date != end.Date) return false;
        if (!Days.TryGetValue(start.DayOfWeek, out var day)) return false;
        var opens = start.Date + day.Opens.ToTimeSpan();
        var closes = start.Date + day.Closes.ToTimeSpan();
        return start >= opens && end <= closes;
    }
}

/// <summary>Everything the planner needs to know about one dealership (A4: resources belong to one dealership).</summary>
public sealed record DealershipProfile(
    int Id,
    string Name,
    TimeZoneInfo TimeZone,
    OpeningHours Hours,
    IReadOnlyList<Technician> Technicians,
    IReadOnlyList<Bay> Bays);
