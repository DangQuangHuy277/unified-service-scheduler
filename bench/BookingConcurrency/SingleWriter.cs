using System.Threading.Channels;
using Npgsql;

namespace BookingConcurrency;

/// <summary>
/// O2: commands are routed to a partition by dealership; one worker owns each partition, decides
/// from an in-memory schedule and writes each drained batch in one transaction. The exclusion
/// constraints stay as a backstop. An in-process channel stands in for the broker, with a fixed
/// <paramref name="brokerHop"/> delay per command to model the network hop.
/// </summary>
public sealed class SingleWriter : IBookingStrategy
{
    private readonly NpgsqlDataSource _db;
    private readonly InFlightGauge _gauge;
    private readonly TimeSpan _brokerHop;
    private readonly int _maxBatch;
    private readonly Channel<Command>[] _partitions;
    private readonly Dictionary<int, DealershipSchedule>[] _schedules;
    private readonly Task[] _workers;
    private long _backgroundTransactions;
    private int _queued;
    private int _peakQueued;

    public SingleWriter(NpgsqlDataSource db, InFlightGauge gauge, int partitions, TimeSpan brokerHop, int maxBatch)
    {
        _db = db;
        _gauge = gauge;
        _brokerHop = brokerHop;
        _maxBatch = maxBatch;
        _partitions = Enumerable.Range(0, partitions)
            .Select(_ => Channel.CreateUnbounded<Command>(new() { SingleReader = true }))
            .ToArray();
        _schedules = _partitions.Select(_ => new Dictionary<int, DealershipSchedule>()).ToArray();
        _workers = Enumerable.Range(0, partitions).Select(i => Task.Run(() => RunWorkerAsync(i))).ToArray();
    }

    public string Name => "o2-single-writer";
    public long BackgroundDbTransactions => Interlocked.Read(ref _backgroundTransactions);
    public int PeakQueueDepth => Volatile.Read(ref _peakQueued);

    /// <summary>
    /// A long-running worker already holds its dealerships' schedules, so load them before the
    /// clock starts. The cold-start cost (after a deploy or rebalance) is a separate concern.
    /// </summary>
    public async Task PrepareAsync(World world)
    {
        for (var d = 1; d <= world.Dealerships; d++)
            _schedules[d % _partitions.Length][d] = await LoadAsync(d, counted: false);
    }

    public async Task<BookingResult> BookAsync(BookingRequest r, CancellationToken ct)
    {
        if (_brokerHop > TimeSpan.Zero) await Task.Delay(_brokerHop, ct);
        var command = new Command(r);
        var depth = Interlocked.Increment(ref _queued);
        int seen;
        while (depth > (seen = Volatile.Read(ref _peakQueued)) &&
               Interlocked.CompareExchange(ref _peakQueued, depth, seen) != seen) { }
        _partitions[r.DealershipId % _partitions.Length].Writer.TryWrite(command);
        return await command.Done.Task.WaitAsync(ct);
    }

    private async Task RunWorkerAsync(int partition)
    {
        var reader = _partitions[partition].Reader;
        var schedules = _schedules[partition];
        var batch = new List<Command>(_maxBatch);
        var booked = new List<(Command Cmd, int Tech, int Bay)>(_maxBatch);

        while (await reader.WaitToReadAsync())
        {
            batch.Clear();
            booked.Clear();
            while (batch.Count < _maxBatch && reader.TryRead(out var c)) batch.Add(c);
            Interlocked.Add(ref _queued, -batch.Count);

            try
            {
                foreach (var c in batch)
                {
                    var r = c.Request;
                    if (!schedules.TryGetValue(r.DealershipId, out var schedule))
                        schedules[r.DealershipId] = schedule = await LoadAsync(r.DealershipId);
                    if (schedule.TryReserve(r.Start, r.End) is { } pair)
                        booked.Add((c, pair.Tech, pair.Bay));
                    else
                        c.Done.TrySetResult(new BookingResult(Outcome.NoCapacity));
                }
                if (booked.Count > 0) await InsertBatchAsync(booked);
                foreach (var b in booked) b.Cmd.Done.TrySetResult(new BookingResult(Outcome.Confirmed));
            }
            catch (Exception)
            {
                // Should never happen with one writer per dealership. Drop the cached schedules so the
                // next command reloads the truth from the database, and fail this batch.
                foreach (var b in booked) schedules.Remove(b.Cmd.Request.DealershipId);
                foreach (var c in batch)
                    c.Done.TrySetResult(new BookingResult(Outcome.Failed, FailureReason.Other));
            }
        }
    }

    private async Task<DealershipSchedule> LoadAsync(int dealershipId, bool counted = true)
    {
        if (counted) Interlocked.Increment(ref _backgroundTransactions);
        await using var conn = await _db.OpenConnectionAsync();
        using var _ = _gauge.Enter();
        var techs = await ReadIdsAsync(conn, "SELECT id FROM technician WHERE dealership_id = $1 ORDER BY id", dealershipId);
        var bays = await ReadIdsAsync(conn, "SELECT id FROM bay WHERE dealership_id = $1 ORDER BY id", dealershipId);
        var schedule = new DealershipSchedule(techs, bays);

        await using var cmd = new NpgsqlCommand("""
            SELECT technician_id, bay_id, lower(slot), upper(slot) FROM appointment
             WHERE dealership_id = $1 AND slot && tstzrange($2, NULL)
            """, conn);
        cmd.Parameters.Add(new() { Value = dealershipId });
        cmd.Parameters.Add(new() { Value = World.Day0 });
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
            schedule.Add(reader.GetInt32(0), reader.GetInt32(1), reader.GetDateTime(2), reader.GetDateTime(3));
        return schedule;
    }

    private async Task InsertBatchAsync(List<(Command Cmd, int Tech, int Bay)> booked)
    {
        Interlocked.Increment(ref _backgroundTransactions);
        await using var conn = await _db.OpenConnectionAsync();
        using var _ = _gauge.Enter();
        await using var cmd = new NpgsqlCommand("""
            INSERT INTO appointment (dealership_id, technician_id, bay_id, slot)
            SELECT d, t, b, tstzrange(s, e, '[)')
              FROM unnest($1::int[], $2::int[], $3::int[], $4::timestamptz[], $5::timestamptz[]) AS x(d, t, b, s, e)
            """, conn);
        cmd.Parameters.Add(new() { Value = booked.Select(b => b.Cmd.Request.DealershipId).ToArray() });
        cmd.Parameters.Add(new() { Value = booked.Select(b => b.Tech).ToArray() });
        cmd.Parameters.Add(new() { Value = booked.Select(b => b.Bay).ToArray() });
        cmd.Parameters.Add(new() { Value = booked.Select(b => b.Cmd.Request.Start).ToArray() });
        cmd.Parameters.Add(new() { Value = booked.Select(b => b.Cmd.Request.End).ToArray() });
        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task<int[]> ReadIdsAsync(NpgsqlConnection conn, string sql, int dealershipId)
    {
        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.Add(new() { Value = dealershipId });
        var ids = new List<int>();
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync()) ids.Add(reader.GetInt32(0));
        return ids.ToArray();
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var p in _partitions) p.Writer.TryComplete();
        await Task.WhenAll(_workers);
    }

    private sealed class Command(BookingRequest request)
    {
        public BookingRequest Request { get; } = request;
        public TaskCompletionSource<BookingResult> Done { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
    }
}

/// <summary>One dealership's future bookings, owned by exactly one worker. First-fit, like O1.</summary>
public sealed class DealershipSchedule(int[] technicians, int[] bays)
{
    private readonly Dictionary<(int Id, DateTime Day), List<(DateTime Start, DateTime End)>> _techBusy = new();
    private readonly Dictionary<(int Id, DateTime Day), List<(DateTime Start, DateTime End)>> _bayBusy = new();

    public (int Tech, int Bay)? TryReserve(DateTime start, DateTime end)
    {
        var tech = FirstFree(technicians, _techBusy, start, end);
        if (tech < 0) return null;
        var bay = FirstFree(bays, _bayBusy, start, end);
        if (bay < 0) return null;
        Add(tech, bay, start, end);
        return (tech, bay);
    }

    public void Add(int tech, int bay, DateTime start, DateTime end)
    {
        Busy(_techBusy, tech, start).Add((start, end));
        Busy(_bayBusy, bay, start).Add((start, end));
    }

    private static int FirstFree(int[] ids, Dictionary<(int, DateTime), List<(DateTime, DateTime)>> busy,
        DateTime start, DateTime end)
    {
        foreach (var id in ids)
        {
            if (!busy.TryGetValue((id, start.Date), out var taken)) return id;
            var free = true;
            foreach (var (s, e) in taken)
                if (s < end && start < e) { free = false; break; }   // half-open overlap, as tstzrange '[)'
            if (free) return id;
        }
        return -1;
    }

    private static List<(DateTime, DateTime)> Busy(Dictionary<(int, DateTime), List<(DateTime, DateTime)>> busy,
        int id, DateTime start)
    {
        if (!busy.TryGetValue((id, start.Date), out var list)) busy[(id, start.Date)] = list = new();
        return list;
    }
}
