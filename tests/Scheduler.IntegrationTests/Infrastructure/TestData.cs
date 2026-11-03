using System.Net.Http.Json;
using Npgsql;
using Scheduler.Api;

namespace Scheduler.IntegrationTests.Infrastructure;

/// <summary>Reference data and requests for tests. Dealerships run on UTC and open every day 08:00–18:00.</summary>
public static class TestData
{
    public const string Oil = "OIL";
    public const string EvBattery = "EV-BATT";

    /// <summary>A date well in the future, so "in the past" never trips a test.</summary>
    public static readonly DateOnly Day = DateOnly.FromDateTime(DateTime.UtcNow).AddDays(30);

    public static DateTimeOffset At(int hour, int minute = 0) =>
        new(Day.ToDateTime(new TimeOnly(hour, minute)), TimeSpan.Zero);

    public static async Task SeedServicesAsync(NpgsqlDataSource db)
    {
        await using var cmd = db.CreateCommand("""
            INSERT INTO service_type (code, name, duration_minutes, required_skill) VALUES
              ('OIL', 'Oil change', 60, 'general'),
              ('EV-BATT', 'EV battery check', 120, 'ev')
            """);
        await cmd.ExecuteNonQueryAsync();
    }

    /// <summary>Technician ids are dealership*100 + n, bay ids dealership*100 + 50 + n.</summary>
    public static async Task SeedDealershipAsync(NpgsqlDataSource db, int id, string[][] technicianSkills, int bays)
    {
        await using var batch = db.CreateBatch();
        batch.BatchCommands.Add(new($"INSERT INTO dealership (id, name, time_zone) VALUES ({id}, 'Dealer {id}', 'Etc/UTC')"));
        batch.BatchCommands.Add(new($"INSERT INTO opening_hours (dealership_id, weekday, opens, closes) SELECT {id}, d, '08:00', '18:00' FROM generate_series(0, 6) d"));
        for (var n = 1; n <= technicianSkills.Length; n++)
        {
            var command = new NpgsqlBatchCommand("INSERT INTO technician (id, dealership_id, name, skills) VALUES ($1, $2, $3, $4)");
            command.Parameters.Add(new() { Value = id * 100 + n });
            command.Parameters.Add(new() { Value = id });
            command.Parameters.Add(new() { Value = $"Tech {id}-{n}" });
            command.Parameters.Add(new() { Value = technicianSkills[n - 1] });
            batch.BatchCommands.Add(command);
        }
        for (var n = 1; n <= bays; n++)
            batch.BatchCommands.Add(new($"INSERT INTO bay (id, dealership_id, name) VALUES ({id * 100 + 50 + n}, {id}, 'Bay {n}')"));
        await batch.ExecuteNonQueryAsync();
    }

    public static string[][] General(int count) => Enumerable.Range(0, count).Select(_ => new[] { "general" }).ToArray();

    public static BookAppointmentRequest Request(int dealershipId, DateTimeOffset start, string service = Oil, int customer = 1) =>
        new()
        {
            DealershipId = dealershipId,
            ServiceType = service,
            Start = start,
            Customer = new CustomerInput { Name = $"Customer {customer}", Email = $"customer{customer}@example.com" },
            Vehicle = new VehicleInput { Vin = Vin(customer), Make = "Toyota", Model = "Corolla" },
        };

    /// <summary>A valid, distinct 17-character VIN per customer number.</summary>
    public static string Vin(int n) => $"JTDBR32E{n:D9}";

    public static Task<HttpResponseMessage> BookAsync(this HttpClient api, BookAppointmentRequest request,
        string? idempotencyKey = null, string? prefer = null)
    {
        var message = new HttpRequestMessage(HttpMethod.Post, "/appointments") { Content = JsonContent.Create(request) };
        if (idempotencyKey is not null) message.Headers.Add("Idempotency-Key", idempotencyKey);
        // Tests that assert a final outcome ask for it explicitly instead of relying on the 2 s default,
        // which a cold start on a loaded machine can exceed (the response would then be a valid 202).
        message.Headers.Add("Prefer", prefer ?? "wait=10");
        return api.SendAsync(message);
    }

    /// <summary>Overlapping bookings that slipped past everything (the benchmark's check). Must always be 0.</summary>
    public static async Task<long> CountOverlapsAsync(NpgsqlDataSource db)
    {
        await using var cmd = db.CreateCommand("""
            SELECT (SELECT count(*) FROM appointment a JOIN appointment b
                      ON a.id < b.id AND a.technician_id = b.technician_id AND a.slot && b.slot)
                 + (SELECT count(*) FROM appointment a JOIN appointment b
                      ON a.id < b.id AND a.bay_id = b.bay_id AND a.slot && b.slot)
            """);
        return (long)(await cmd.ExecuteScalarAsync())!;
    }

    public static async Task<long> CountAsync(NpgsqlDataSource db, string table)
    {
        await using var cmd = db.CreateCommand($"SELECT count(*) FROM {table}");
        return (long)(await cmd.ExecuteScalarAsync())!;
    }
}
