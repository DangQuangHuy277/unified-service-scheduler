using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using Scheduler.Api;
using Scheduler.IntegrationTests.Infrastructure;
using static Scheduler.IntegrationTests.Infrastructure.TestData;

namespace Scheduler.IntegrationTests;

/// <summary>End to end: HTTP → Kafka → owning worker → Postgres → NOTIFY → HTTP.</summary>
public sealed class BookingFlowTests(Containers containers) : IAsyncLifetime
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private TestEnvironment _env = null!;

    public async ValueTask InitializeAsync() =>
        _env = await TestEnvironment.CreateAsync(containers, async db =>
        {
            await SeedServicesAsync(db);
            await SeedDealershipAsync(db, 1, General(3), bays: 2);
            await SeedDealershipAsync(db, 2, [["general"], ["general", "ev"]], bays: 3);
            await SeedDealershipAsync(db, 3, General(8), bays: 6);
        });

    public async ValueTask DisposeAsync() => await _env.DisposeAsync();

    [Fact]
    public async Task Confirmed_appointment_links_customer_vehicle_technician_and_bay()
    {
        var api = _env.Api;

        var response = await api.BookAsync(Request(1, At(10), customer: 7));

        Assert.Equal(HttpStatusCode.Created, response.StatusCode);
        var created = (await response.Content.ReadFromJsonAsync<AppointmentResponse>(Ct))!;
        Assert.Equal(1, created.Dealership.Id);
        Assert.Equal(Oil, created.Service.Id);
        Assert.Equal(At(10), created.Start);
        Assert.Equal(At(11), created.End);
        Assert.Equal(101, created.Technician.Id);
        Assert.Equal(151, created.Bay.Id);
        Assert.Equal("customer7@example.com", created.Customer.Email);
        Assert.Equal(Vin(7), created.Vehicle.Vin);
        Assert.Equal($"/appointments/{created.Id}", response.Headers.Location!.OriginalString);

        var fetched = await api.GetFromJsonAsync<AppointmentResponse>(response.Headers.Location, Ct);
        Assert.Equal(created, fetched);   // persisted, not just echoed (R3)
    }

    [Fact]
    public async Task Burst_for_one_slot_books_exactly_the_capacity_and_never_overlaps()
    {
        // Dealership 3: 8 qualified technicians but only 6 bays, so 6 of 50 simultaneous requests can fit.
        var api = _env.Api;

        var responses = await Task.WhenAll(Enumerable.Range(1, 50).Select(i => api.BookAsync(Request(3, At(9), customer: i))));

        Assert.Equal(6, responses.Count(r => r.StatusCode == HttpStatusCode.Created));
        Assert.Equal(44, responses.Count(r => r.StatusCode == HttpStatusCode.Conflict));
        Assert.Equal(6, await CountAsync(_env.Db, "appointment"));
        Assert.Equal(0, await CountOverlapsAsync(_env.Db));
    }

    [Fact]
    public async Task Only_a_qualified_technician_is_assigned_even_when_others_are_free()
    {
        // Dealership 2: tech 201 (general) comes first by id but can't do EV work; 202 can. Three bays.
        var api = _env.Api;

        var first = await api.BookAsync(Request(2, At(10), EvBattery, customer: 1));
        var second = await api.BookAsync(Request(2, At(10), EvBattery, customer: 2));

        Assert.Equal(HttpStatusCode.Created, first.StatusCode);
        Assert.Equal(202, (await first.Content.ReadFromJsonAsync<AppointmentResponse>(Ct))!.Technician.Id);
        Assert.Equal(HttpStatusCode.Conflict, second.StatusCode);
        Assert.Equal("NoCapacity", await OutcomeAsync(second));
    }

    [Fact]
    public async Task Retrying_with_the_same_idempotency_key_returns_the_same_appointment()
    {
        var api = _env.Api;
        var request = Request(1, At(14));

        var first = await api.BookAsync(request, idempotencyKey: "retry-me");
        var retry = await api.BookAsync(request, idempotencyKey: "retry-me");

        Assert.Equal(HttpStatusCode.Created, first.StatusCode);
        Assert.Equal(HttpStatusCode.Created, retry.StatusCode);
        Assert.Equal(first.Headers.Location, retry.Headers.Location);
        Assert.Equal(1, await CountAsync(_env.Db, "appointment"));
    }

    [Fact]
    public async Task Respond_async_answers_202_and_the_request_can_be_polled_to_its_outcome()
    {
        var api = _env.Api;

        var accepted = await api.BookAsync(Request(1, At(15)), prefer: "respond-async");

        Assert.Equal(HttpStatusCode.Accepted, accepted.StatusCode);
        var location = accepted.Headers.Location!;
        Assert.StartsWith("/booking-requests/", location.OriginalString);
        BookingRequestStatus? status = null;
        await Eventually.TrueAsync(async () =>
        {
            status = await api.GetFromJsonAsync<BookingRequestStatus>(location, Ct);
            return status!.Status != "pending";
        }, TimeSpan.FromSeconds(10), "the request to be decided");
        Assert.Equal("confirmed", status!.Status);
        Assert.NotNull(status.AppointmentId);
    }

    [Fact]
    public async Task Malformed_input_is_400_and_unbookable_requests_are_422()
    {
        var api = _env.Api;

        var badVin = await api.BookAsync(Request(1, At(10)) with { Vehicle = new VehicleInput { Vin = "NOT-A-VIN", Make = "Ford", Model = "Focus" } });
        var noEmail = await api.BookAsync(Request(1, At(10)) with { Customer = new CustomerInput { Name = "No Email" } });
        var availabilityWithoutDate = await api.GetAsync($"/availability?dealershipId=1&serviceType={Oil}", Ct);
        var afterClosing = await api.BookAsync(Request(1, At(17, 30)));
        var unknownService = await api.BookAsync(Request(1, At(10), service: "TELEPORT"));

        Assert.Equal(HttpStatusCode.BadRequest, badVin.StatusCode);
        Assert.Contains("vehicle.vin", await ErrorKeysAsync(badVin));   // keyed by the JSON names clients send
        Assert.Contains("customer.email", await ErrorKeysAsync(noEmail));
        Assert.Contains("date", await ErrorKeysAsync(availabilityWithoutDate));
        Assert.Equal(HttpStatusCode.UnprocessableEntity, afterClosing.StatusCode);
        Assert.Equal("OutsideOpeningHours", await OutcomeAsync(afterClosing));
        Assert.Equal(HttpStatusCode.UnprocessableEntity, unknownService.StatusCode);
        Assert.Equal(0, await CountAsync(_env.Db, "appointment"));
    }

    [Fact]
    public async Task Availability_drops_a_slot_once_the_bookings_are_projected()
    {
        var api = _env.Api;
        var url = $"/availability?dealershipId=1&serviceType={Oil}&date={Day:yyyy-MM-dd}";
        var before = await api.GetFromJsonAsync<AvailabilityResponse>(url, Ct);
        Assert.Contains(before!.Slots, s => s.Start == At(12));

        // Dealership 1 has two bays: fill both at 12:00.
        foreach (var customer in new[] { 1, 2 })
            Assert.Equal(HttpStatusCode.Created, (await api.BookAsync(Request(1, At(12), customer: customer))).StatusCode);

        // Outbox → Kafka → projector → read model: eventually consistent, typically well under a second.
        await Eventually.TrueAsync(async () =>
        {
            var after = await api.GetFromJsonAsync<AvailabilityResponse>(url, Ct);
            return after!.Slots.All(s => s.Start != At(12)) && after.Slots.Any(s => s.Start == At(13));
        }, TimeSpan.FromSeconds(15), "the read model to show 12:00 as taken");
    }

    private static async Task<string[]> ErrorKeysAsync(HttpResponseMessage response)
    {
        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        using var problem = JsonDocument.Parse(await response.Content.ReadAsStringAsync(Ct));
        return problem.RootElement.GetProperty("errors").EnumerateObject().Select(e => e.Name).ToArray();
    }

    private static async Task<string?> OutcomeAsync(HttpResponseMessage response)
    {
        using var problem = JsonDocument.Parse(await response.Content.ReadAsStringAsync(Ct));
        return problem.RootElement.TryGetProperty("outcome", out var outcome) ? outcome.GetString() : null;
    }
}
