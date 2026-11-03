using Scheduler.Domain;

namespace Scheduler.UnitTests;

public class DealershipScheduleTests
{
    private static readonly TimeZoneInfo London = TimeZoneInfo.FindSystemTimeZoneById("Europe/London");
    private static readonly DateTime LongAgo = new(2020, 1, 1, 0, 0, 0, DateTimeKind.Utc);

    private static readonly ServiceType OilChange = new("OIL", "Oil change", TimeSpan.FromHours(1), "general");
    private static readonly ServiceType EvBattery = new("EV-BATT", "EV battery check", TimeSpan.FromHours(2), "ev");

    /// <summary>Thursday 2027-03-04, outside daylight saving: London local time equals UTC.</summary>
    private static DateTime Winter(int hour, int minute = 0) => new(2027, 3, 4, hour, minute, 0, DateTimeKind.Utc);

    private static Technician Tech(int id, params string[] skills) => new(id, $"Tech {id}", skills.ToHashSet());

    private static DealershipSchedule Schedule(Technician[] technicians, int bays, OpeningHours? hours = null) =>
        new(new DealershipProfile(1, "Test Motors", London, hours ?? OpeningHours.EveryDay(new(8, 0), new(18, 0)),
            technicians, Enumerable.Range(1, bays).Select(i => new Bay(100 + i, $"Bay {i}")).ToArray()));

    [Fact]
    public void Confirms_the_first_free_qualified_technician_and_bay()
    {
        var schedule = Schedule([Tech(1, "general"), Tech(2, "general")], bays: 2);

        var decision = schedule.TryReserve(OilChange, Winter(10), LongAgo);

        Assert.Equal(BookingOutcome.Confirmed, decision.Outcome);
        Assert.Equal(1, decision.TechnicianId);
        Assert.Equal(101, decision.BayId);
        Assert.Equal(new TimeSlot(Winter(10), Winter(11)), decision.Slot);
    }

    [Fact]
    public void Never_assigns_a_technician_without_the_required_skill()
    {
        // Tech 1 is free and comes first by id, but can't do EV work.
        var schedule = Schedule([Tech(1, "general"), Tech(2, "general", "ev")], bays: 3);

        var first = schedule.TryReserve(EvBattery, Winter(9), LongAgo);
        var second = schedule.TryReserve(EvBattery, Winter(9), LongAgo);

        Assert.Equal(2, first.TechnicianId);
        Assert.Equal(BookingOutcome.NoCapacity, second.Outcome);   // a bay is free, the only EV tech is not
    }

    [Fact]
    public void Rejects_when_no_technician_has_the_skill_at_all()
    {
        var schedule = Schedule([Tech(1, "general")], bays: 1);

        Assert.Equal(BookingOutcome.NoQualifiedTechnician, schedule.TryReserve(EvBattery, Winter(9), LongAgo).Outcome);
    }

    [Fact]
    public void Runs_out_of_bays_before_technicians()
    {
        var schedule = Schedule([Tech(1, "general"), Tech(2, "general"), Tech(3, "general")], bays: 2);

        var outcomes = Enumerable.Range(0, 3).Select(_ => schedule.TryReserve(OilChange, Winter(10), LongAgo).Outcome);

        Assert.Equal([BookingOutcome.Confirmed, BookingOutcome.Confirmed, BookingOutcome.NoCapacity], outcomes);
    }

    [Fact]
    public void A_partial_overlap_blocks_the_whole_service_duration()
    {
        var schedule = Schedule([Tech(1, "general", "ev")], bays: 1);
        schedule.TryReserve(OilChange, Winter(11), LongAgo);   // 11:00–12:00

        // A 2-hour job from 10:00 would run until 12:00 and collide during its second hour.
        Assert.Equal(BookingOutcome.NoCapacity, schedule.TryReserve(EvBattery, Winter(10), LongAgo).Outcome);
    }

    [Fact]
    public void Back_to_back_jobs_do_not_clash_because_slots_are_half_open()
    {
        var schedule = Schedule([Tech(1, "general")], bays: 1);

        var first = schedule.TryReserve(OilChange, Winter(9), LongAgo);    // [09:00, 10:00)
        var second = schedule.TryReserve(OilChange, Winter(10), LongAgo);  // [10:00, 11:00)

        Assert.True(first.IsConfirmed);
        Assert.True(second.IsConfirmed);
        Assert.Equal(first.TechnicianId, second.TechnicianId);
    }

    [Fact]
    public void Respects_appointments_loaded_from_storage()
    {
        var schedule = Schedule([Tech(1, "general")], bays: 2);
        schedule.Add(technicianId: 1, bayId: 101, new TimeSlot(Winter(9, 30), Winter(10, 30)));

        Assert.Equal(BookingOutcome.NoCapacity, schedule.TryReserve(OilChange, Winter(10), LongAgo).Outcome);
    }

    [Fact]
    public void First_fit_makes_the_same_choices_for_the_same_request_sequence()
    {
        static (int?, int?)[] Replay()
        {
            var schedule = Schedule([Tech(3, "general"), Tech(1, "general"), Tech(2, "general")], bays: 3);
            return [.. new[] { 9, 9, 10, 9 }.Select(h => schedule.TryReserve(OilChange, Winter(h), LongAgo))
                .Select(d => (d.TechnicianId, d.BayId))];
        }

        Assert.Equal(Replay(), Replay());
        Assert.Equal(((int?)1, (int?)101), Replay()[0]);   // lowest id first, whatever order the profile lists them in
    }

    [Fact]
    public void Rejects_a_start_in_the_past()
    {
        var schedule = Schedule([Tech(1, "general")], bays: 1);

        Assert.Equal(BookingOutcome.InThePast, schedule.TryReserve(OilChange, Winter(10), nowUtc: Winter(10, 1)).Outcome);
    }

    [Theory]
    [InlineData(7, 30)]    // starts before opening
    [InlineData(17, 30)]   // would end at 18:30, after closing
    public void Rejects_slots_outside_opening_hours(int hour, int minute)
    {
        var schedule = Schedule([Tech(1, "general")], bays: 1);

        Assert.Equal(BookingOutcome.OutsideOpeningHours, schedule.TryReserve(OilChange, Winter(hour, minute), LongAgo).Outcome);
    }

    [Fact]
    public void Accepts_a_job_that_ends_exactly_at_closing_time()
    {
        var schedule = Schedule([Tech(1, "general")], bays: 1);

        Assert.True(schedule.TryReserve(OilChange, Winter(17), LongAgo).IsConfirmed);
    }

    [Fact]
    public void Rejects_days_the_dealership_is_closed()
    {
        var weekdaysOnly = new OpeningHours(Enum.GetValues<DayOfWeek>()
            .Where(d => d is not (DayOfWeek.Saturday or DayOfWeek.Sunday))
            .ToDictionary(d => d, _ => (new TimeOnly(8, 0), new TimeOnly(18, 0))));
        var schedule = Schedule([Tech(1, "general")], bays: 1, weekdaysOnly);
        var saturday = new DateTime(2027, 3, 6, 10, 0, 0, DateTimeKind.Utc);

        Assert.Equal(BookingOutcome.OutsideOpeningHours, schedule.TryReserve(OilChange, saturday, LongAgo).Outcome);
    }

    [Fact]
    public void Opening_hours_follow_daylight_saving_in_the_dealership_time_zone()
    {
        // London moves to BST (UTC+1) on 2027-03-28. 17:00 UTC is 17:00 local the day before, 18:00 local after.
        var schedule = Schedule([Tech(1, "general")], bays: 1);
        var beforeChange = new DateTime(2027, 3, 27, 17, 0, 0, DateTimeKind.Utc);
        var afterChange = new DateTime(2027, 3, 29, 17, 0, 0, DateTimeKind.Utc);
        var bstOpening = new DateTime(2027, 3, 29, 7, 0, 0, DateTimeKind.Utc);   // 08:00 BST

        Assert.True(schedule.TryReserve(OilChange, beforeChange, LongAgo).IsConfirmed);
        Assert.Equal(BookingOutcome.OutsideOpeningHours, schedule.TryReserve(OilChange, afterChange, LongAgo).Outcome);
        Assert.True(schedule.TryReserve(OilChange, bstOpening, LongAgo).IsConfirmed);
    }

    [Fact]
    public void Free_slots_cover_the_day_and_drop_starts_that_would_overlap_a_full_hour()
    {
        var schedule = Schedule([Tech(1, "general")], bays: 1);
        var day = new DateOnly(2027, 3, 4);
        var step = TimeSpan.FromMinutes(15);

        var empty = schedule.FreeSlots(OilChange, day, LongAgo, step);
        schedule.TryReserve(OilChange, Winter(10), LongAgo);   // the only bay is busy 10:00–11:00
        var afterBooking = schedule.FreeSlots(OilChange, day, LongAgo, step);

        Assert.Equal(37, empty.Count);   // 08:00, 08:15 … 17:00
        Assert.Equal(Winter(8), empty[0].Start);
        Assert.Equal(Winter(17), empty[^1].Start);
        // Starts 09:15 … 10:45 would overlap [10:00, 11:00); 09:00 and 11:00 touch it but don't overlap.
        Assert.Equal(30, afterBooking.Count);
        Assert.Contains(afterBooking, s => s.Start == Winter(9));
        Assert.Contains(afterBooking, s => s.Start == Winter(11));
        Assert.DoesNotContain(afterBooking, s => s.Start == Winter(9, 15));
    }

    [Fact]
    public void Free_slots_do_not_reserve_anything()
    {
        var schedule = Schedule([Tech(1, "general")], bays: 1);
        schedule.FreeSlots(OilChange, new DateOnly(2027, 3, 4), LongAgo, TimeSpan.FromMinutes(15));

        Assert.True(schedule.TryReserve(OilChange, Winter(8), LongAgo).IsConfirmed);
    }
}

public class TimeSlotTests
{
    private static DateTime At(int hour) => new(2027, 3, 4, hour, 0, 0, DateTimeKind.Utc);

    [Fact]
    public void Adjacent_slots_do_not_overlap() =>
        Assert.False(new TimeSlot(At(9), At(10)).Overlaps(new TimeSlot(At(10), At(11))));

    [Fact]
    public void Partially_covering_slots_overlap() =>
        Assert.True(new TimeSlot(At(9), At(11)).Overlaps(new TimeSlot(At(10), At(12))));

    [Fact]
    public void Rejects_non_utc_bounds() =>
        Assert.Throws<ArgumentException>(() => new TimeSlot(DateTime.SpecifyKind(At(9), DateTimeKind.Local), At(10)));

    [Fact]
    public void Rejects_empty_or_inverted_slots() =>
        Assert.Throws<ArgumentException>(() => new TimeSlot(At(10), At(10)));
}
