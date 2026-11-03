namespace BookingConcurrency;

/// <summary>The fleet the requests target. Technician and bay ids are dealership * 1000 + n.</summary>
public sealed record World(
    string Name,
    int Dealerships,
    int TechniciansPerDealership,
    int BaysPerDealership,
    int HorizonDays,
    int HistoryDays)
{
    /// <summary>Day 0 of the booking horizon; history sits before it.</summary>
    public static readonly DateTime Day0 = new(2027, 3, 1, 0, 0, 0, DateTimeKind.Utc);

    public static int TechnicianId(int dealership, int n) => dealership * 1000 + n;
    public static int BayId(int dealership, int n) => dealership * 1000 + n;
}

public sealed record BookingRequest(int DealershipId, int Day, DateTime Start, DateTime End);

public enum Outcome { Confirmed, NoCapacity, Failed, Unfinished }

public enum FailureReason { None, ConflictRetriesExhausted, DeadlockRetriesExhausted, PoolTimeout, Other }

public readonly record struct BookingResult(
    Outcome Outcome,
    FailureReason Reason = FailureReason.None,
    int DbTransactions = 0,
    int ConflictRetries = 0,
    int DeadlockRetries = 0);

/// <summary>Every approach answers the same question: book this slot, or say why not.</summary>
public interface IBookingStrategy : IAsyncDisposable
{
    string Name { get; }
    Task<BookingResult> BookAsync(BookingRequest request, CancellationToken ct);

    /// <summary>Untimed set-up before the run (O2 loads its in-memory schedules here).</summary>
    Task PrepareAsync(World world) => Task.CompletedTask;

    /// <summary>Transactions spent outside individual requests (O2 batches and schedule loads).</summary>
    long BackgroundDbTransactions => 0;
    int PeakQueueDepth => 0;
}

/// <summary>Tracks how many database operations run at once (≈ pool connections in use).</summary>
public sealed class InFlightGauge
{
    private int _current;
    private int _peak;

    public int Peak => Volatile.Read(ref _peak);

    public IDisposable Enter()
    {
        var now = Interlocked.Increment(ref _current);
        int seen;
        while (now > (seen = Volatile.Read(ref _peak)) &&
               Interlocked.CompareExchange(ref _peak, now, seen) != seen) { }
        return new Exit(this);
    }

    private sealed class Exit(InFlightGauge gauge) : IDisposable
    {
        public void Dispose() => Interlocked.Decrement(ref gauge._current);
    }
}
