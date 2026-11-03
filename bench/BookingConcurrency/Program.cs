using BookingConcurrency;
using Npgsql;

var options = Options.Parse(args);
ThreadPool.SetMinThreads(256, 256);

await using var db = new NpgsqlDataSourceBuilder(options.ConnectionString).Build();
var runner = new Runner(db, options);
var suite = Suite.Build(options);

Directory.CreateDirectory(Path.GetDirectoryName(Path.GetFullPath(options.Output))!);
await using var csv = new StreamWriter(options.Output, append: false);
await csv.WriteLineAsync(Runner.CsvHeader);

World? current = null;
foreach (var spec in suite)
{
    if (spec.World != current)
    {
        Console.WriteLine($"== world '{spec.World.Name}': creating schema");
        await Database.CreateSchemaAsync(db, spec.World);
        await Database.SeedHistoryAsync(db, spec.World);
        current = spec.World;
        if (options.Warmup) await Suite.WarmUpAsync(runner, spec);
    }
    var row = await runner.RunAsync(spec);
    await csv.WriteLineAsync(row);
    await csv.FlushAsync();
    Console.WriteLine(row);
}

public sealed record Options(string ConnectionString, string Output, string SuiteName, string[] Approaches,
    string[] Scenarios, double DurationSeconds, TimeSpan DrainTimeout, int Partitions, double BrokerHopMs, int MaxBatch,
    bool Warmup)
{
    public static Options Parse(string[] args)
    {
        string Get(string name, string fallback)
        {
            var i = Array.IndexOf(args, "--" + name);
            return i >= 0 && i + 1 < args.Length ? args[i + 1] : fallback;
        }
        return new Options(
            Get("conn", "Host=127.0.0.1;Port=55433;Username=postgres;Password=bench;Database=postgres;" +
                        "Maximum Pool Size=50;Timeout=30;Command Timeout=60;No Reset On Close=true"),
            Get("out", "results/results.csv"),
            Get("suite", "full"),
            Get("approaches", "o1-naive,o1-lock,o2-single-writer").Split(','),
            Get("scenarios", "normal,hot,burst").Split(','),
            double.Parse(Get("duration", "10")),
            TimeSpan.FromSeconds(double.Parse(Get("drain", "30"))),
            int.Parse(Get("partitions", "16")),
            double.Parse(Get("broker-hop-ms", "5")),
            int.Parse(Get("max-batch", "64")),
            Get("warmup", "true") == "true");
    }
}

public static class Suite
{
    // Normal day: requests spread over many dealerships, so true conflicts are rare.
    private static readonly World Normal = new("normal", Dealerships: 500, TechniciansPerDealership: 8,
        BaysPerDealership: 6, HorizonDays: 30, HistoryDays: 7);

    // Hot dealership: one large service centre takes all the traffic (a recall campaign).
    private static readonly World Hot = new("hot", Dealerships: 1, TechniciansPerDealership: 40,
        BaysPerDealership: 30, HorizonDays: 60, HistoryDays: 7);

    // Burst: everyone asks for the same dealership, day and hour at the same instant.
    private static readonly World Burst = new("burst", Dealerships: 1, TechniciansPerDealership: 8,
        BaysPerDealership: 6, HorizonDays: 1, HistoryDays: 7);

    public static IEnumerable<RunSpec> Build(Options o)
    {
        var smoke = o.SuiteName == "smoke";
        var rates = new Dictionary<string, double[]>
        {
            ["normal"] = smoke ? [500] : [250, 500, 1000, 2000, 4000, 8000],
            ["hot"] = smoke ? [500] : [100, 250, 500, 1000, 2000, 4000],
        };
        int[] burstSizes = smoke ? [50] : [10, 25, 50, 100, 200];
        var trials = smoke ? 1 : 3;

        foreach (var scenario in o.Scenarios)
        {
            if (scenario == "burst")
            {
                foreach (var size in burstSizes)
                    for (var trial = 1; trial <= trials; trial++)
                        foreach (var approach in o.Approaches)
                            yield return BurstSpec(approach, size, trial);
                continue;
            }
            var world = scenario == "hot" ? Hot : Normal;
            foreach (var rate in rates[scenario])
                for (var trial = 1; trial <= trials; trial++)          // approaches interleaved per trial
                    foreach (var approach in o.Approaches)
                        yield return SteadySpec(scenario, world, approach, rate, o.DurationSeconds) with { Trial = trial };
        }
    }

    /// <summary>Same seed per scenario and rate, so every approach faces the identical request stream.</summary>
    private static RunSpec SteadySpec(string scenario, World world, string approach, double rps, double seconds)
    {
        var rng = new Random(scenario.Length * 100_003 + (int)rps);   // deterministic across runs
        var n = (int)(rps * seconds);
        var requests = new BookingRequest[n];
        var arrivals = new double[n];
        var t = 0.0;
        for (var i = 0; i < n; i++)
        {
            t += -Math.Log(1 - rng.NextDouble()) * 1000.0 / rps;   // Poisson arrivals
            arrivals[i] = t;
            requests[i] = RandomRequest(rng, world);
        }
        return new RunSpec(scenario, world, approach, rps, 0, 1, requests, arrivals);
    }

    private static RunSpec BurstSpec(string approach, int size, int trial)
    {
        var start = World.Day0.AddHours(9);
        var requests = Enumerable.Repeat(new BookingRequest(1, 0, start, start.AddHours(1)), size).ToArray();
        return new RunSpec("burst", Burst, approach, 0, size, trial, requests, new double[size]);
    }

    private static BookingRequest RandomRequest(Random rng, World world)
    {
        var dealership = rng.Next(1, world.Dealerships + 1);
        var day = rng.Next(world.HorizonDays);
        var hours = rng.NextDouble() < 0.7 ? 1 : 2;                 // 70% one-hour jobs, 30% two-hour
        var startHour = rng.Next(8, 18 - hours + 1);                // workshop open 08:00-18:00
        var start = World.Day0.AddDays(day).AddHours(startHour);
        return new BookingRequest(dealership, day, start, start.AddHours(hours));
    }

    /// <summary>JIT and connection-pool warm-up for every approach; results are discarded.</summary>
    public static async Task WarmUpAsync(Runner runner, RunSpec like)
    {
        foreach (var approach in new[] { "o1-naive", "o1-lock", "o2-single-writer" })
            await runner.RunAsync(SteadySpec("warmup", like.World, approach, 200, 2));
    }
}
