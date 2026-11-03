using System.Diagnostics;
using System.Globalization;
using Npgsql;

namespace BookingConcurrency;

public sealed record RunSpec(string Scenario, World World, string Approach, double OfferedRps, int BurstSize, int Trial,
    BookingRequest[] Requests, double[] ArrivalMs);

/// <summary>
/// Open-loop load: request i is due at ArrivalMs[i] whether or not earlier ones have finished, and
/// its latency is measured from that due time. A closed loop would hide queueing (coordinated omission).
/// </summary>
public sealed class Runner(NpgsqlDataSource db, Options options)
{
    public const double SloMs = 500;

    public async Task<string> RunAsync(RunSpec spec)
    {
        await Database.ResetAppointmentsAsync(db);
        var gauge = new InFlightGauge();
        await using var strategy = Create(spec.Approach, gauge);
        await strategy.PrepareAsync(spec.World);

        var n = spec.Requests.Length;
        var results = new BookingResult[n];
        var latency = new double[n];
        var done = new double[n];
        var tasks = new Task[n];
        using var drain = new CancellationTokenSource();
        var cpu0 = Process.GetCurrentProcess().TotalProcessorTime;
        var clock = Stopwatch.StartNew();

        for (var next = 0; next < n;)
        {
            var now = clock.Elapsed.TotalMilliseconds;
            while (next < n && spec.ArrivalMs[next] <= now)
            {
                var i = next++;
                tasks[i] = Task.Run(() => ExecuteAsync(i));
            }
            if (next < n) await Task.Delay(1);
        }
        var all = Task.WhenAll(tasks);
        if (await Task.WhenAny(all, Task.Delay(options.DrainTimeout)) != all) drain.Cancel();
        await all;
        var wallMs = done.Max();
        var cpuCores = (Process.GetCurrentProcess().TotalProcessorTime - cpu0).TotalMilliseconds / clock.Elapsed.TotalMilliseconds;
        var (rows, techOverlaps, bayOverlaps) = await Database.VerifyAsync(db);

        return Summarize(spec, strategy, gauge, results, latency, wallMs, cpuCores, rows, techOverlaps + bayOverlaps);

        async Task ExecuteAsync(int i)
        {
            BookingResult r;
            try { r = await strategy.BookAsync(spec.Requests[i], drain.Token); }
            catch (OperationCanceledException) { r = new BookingResult(Outcome.Unfinished); }
            catch (Exception) { r = new BookingResult(Outcome.Failed, FailureReason.Other); }
            var t = clock.Elapsed.TotalMilliseconds;
            results[i] = r;
            latency[i] = t - spec.ArrivalMs[i];
            done[i] = t;
        }
    }

    private IBookingStrategy Create(string approach, InFlightGauge gauge) => approach switch
    {
        "o1-naive" => new DatabaseEnforced(db, gauge, lockDealershipDay: false),
        "o1-lock" => new DatabaseEnforced(db, gauge, lockDealershipDay: true),
        "o2-single-writer" => new SingleWriter(db, gauge, options.Partitions,
            TimeSpan.FromMilliseconds(options.BrokerHopMs), options.MaxBatch),
        _ => throw new ArgumentException($"Unknown approach '{approach}'"),
    };

    public static readonly string CsvHeader = string.Join(',',
        "scenario", "approach", "offered_rps", "burst_size", "trial", "requests",
        "answered", "confirmed", "no_capacity", "failed", "failed_deadlock", "failed_conflict", "failed_pool",
        "failed_other", "unfinished", "achieved_rps", "goodput_rps", "p50_ms", "p90_ms", "p99_ms", "p999_ms", "max_ms",
        "db_tx", "db_tx_per_confirmed", "conflict_retries", "deadlock_retries", "peak_db_in_flight",
        "peak_queue_depth", "client_cpu_cores", "rows_written", "overlaps");

    private static string Summarize(RunSpec spec, IBookingStrategy strategy, InFlightGauge gauge,
        BookingResult[] results, double[] latency, double wallMs, double cpuCores, long rows, long overlaps)
    {
        int Count(Func<BookingResult, bool> p) => results.Count(p);
        var answered = Enumerable.Range(0, results.Length).Where(i => results[i].Outcome != Outcome.Unfinished).ToArray();
        var lat = answered.Select(i => latency[i]).Order().ToArray();
        var good = answered.Count(i => results[i].Outcome is Outcome.Confirmed or Outcome.NoCapacity && latency[i] <= SloMs);
        var confirmed = Count(r => r.Outcome == Outcome.Confirmed);
        var dbTx = results.Sum(r => (long)r.DbTransactions) + strategy.BackgroundDbTransactions;
        var seconds = wallMs / 1000.0;

        // Burst runs fire everything at t=0, so a rate means little there; report 0.
        var isBurst = spec.BurstSize > 0;
        return string.Join(',', new object[]
        {
            spec.Scenario, spec.Approach, isBurst ? 0 : spec.OfferedRps, spec.BurstSize, spec.Trial, results.Length,
            answered.Length, confirmed, Count(r => r.Outcome == Outcome.NoCapacity), Count(r => r.Outcome == Outcome.Failed),
            Count(r => r.Reason == FailureReason.DeadlockRetriesExhausted),
            Count(r => r.Reason == FailureReason.ConflictRetriesExhausted),
            Count(r => r.Reason == FailureReason.PoolTimeout), Count(r => r.Reason == FailureReason.Other),
            Count(r => r.Outcome == Outcome.Unfinished),
            F(isBurst ? 0 : answered.Length / seconds), F(isBurst ? 0 : good / seconds),
            F(Pct(lat, 0.50)), F(Pct(lat, 0.90)), F(Pct(lat, 0.99)), F(Pct(lat, 0.999)), F(lat.LastOrDefault()),
            dbTx, F(confirmed == 0 ? 0 : (double)dbTx / confirmed),
            results.Sum(r => r.ConflictRetries), results.Sum(r => r.DeadlockRetries),
            gauge.Peak, strategy.PeakQueueDepth, F(cpuCores), rows, overlaps,
        }.Select(v => Convert.ToString(v, CultureInfo.InvariantCulture)));
    }

    private static double Pct(double[] sorted, double p) =>
        sorted.Length == 0 ? 0 : sorted[Math.Min(sorted.Length - 1, (int)Math.Ceiling(p * sorted.Length) - 1)];

    private static string F(double v) => v.ToString("0.###", CultureInfo.InvariantCulture);
}
