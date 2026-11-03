namespace Scheduler.Domain;

/// <summary>
/// A half-open UTC interval <c>[Start, End)</c>, the same convention as Postgres
/// <c>tstzrange(start, end, '[)')</c>: a 09:00–10:00 job and a 10:00–11:00 job don't clash (A7).
/// </summary>
public readonly record struct TimeSlot
{
    public TimeSlot(DateTime start, DateTime end)
    {
        if (start.Kind != DateTimeKind.Utc || end.Kind != DateTimeKind.Utc)
            throw new ArgumentException("TimeSlot bounds must be UTC.");
        if (end <= start)
            throw new ArgumentException($"TimeSlot end {end:O} must be after start {start:O}.");
        Start = start;
        End = end;
    }

    public DateTime Start { get; }
    public DateTime End { get; }
    public TimeSpan Duration => End - Start;

    public static TimeSlot StartingAt(DateTime start, TimeSpan duration) => new(start, start + duration);

    public bool Overlaps(TimeSlot other) => Start < other.End && other.Start < End;

    public override string ToString() => $"[{Start:O}, {End:O})";
}
