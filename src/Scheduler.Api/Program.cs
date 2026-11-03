using System.Text.Json.Nodes;
using Microsoft.AspNetCore.Mvc.ModelBinding.Metadata;
using Microsoft.EntityFrameworkCore;
using OpenTelemetry.Trace;
using Scheduler.Api.Booking;
using Scheduler.Infrastructure;
using Scheduler.Infrastructure.Persistence;

var builder = WebApplication.CreateBuilder(args);

builder.AddSchedulerData()
    .AddSchedulerKafka()
    .AddSchedulerTelemetry("scheduler-api",
        tracing: t => t.AddAspNetCoreInstrumentation(o => o.Filter = http => http.Request.Path != "/health"),
        metrics: m => m.AddMeter("Microsoft.AspNetCore.Hosting", "Microsoft.AspNetCore.Server.Kestrel"));

builder.Services.AddSingleton<DecisionWaiter>();
builder.Services.AddHostedService<DecisionListener>();
builder.Services.AddSingleton<BookingCommandPublisher>();
builder.Services.AddControllers(o =>
    // Validation errors are keyed by the JSON names clients send ("customer.email"), not C# property paths.
    o.ModelMetadataDetailsProviders.Add(new SystemTextJsonValidationMetadataProvider()));
builder.Services.AddProblemDetails();
builder.Services.AddOpenApi(o => o.AddDocumentTransformer((document, _, _) =>
{
    document.Info.Title = "Unified Service Scheduler API";
    document.Info.Description =
        "Books workshop appointments only when a qualified technician and a service bay are both free for the " +
        "whole service duration. POST /appointments answers 201 (confirmed), 409 (no capacity), 422 (cannot be " +
        "booked as asked) or 202 (still deciding: poll the Location). Send Idempotency-Key to make retries safe.";
    return Task.CompletedTask;
}).AddOperationTransformer((operation, context, _) =>
{
    // A ready-to-send booking, so "Try it out" in /swagger works without typing JSON.
    if (context.Description.HttpMethod == "POST" && context.Description.RelativePath == "appointments" &&
        operation.RequestBody?.Content?.TryGetValue("application/json", out var json) == true)
    {
        json.Example = JsonNode.Parse("""
            {
              "dealershipId": 1,
              "serviceType": "OIL",
              "start": "2027-06-16T09:00:00+01:00",
              "customer": { "name": "Ava Thompson", "email": "ava.thompson@example.com" },
              "vehicle": { "vin": "WVWZZZ1KZAW000001", "make": "Volkswagen", "model": "Golf" }
            }
            """);
    }
    return Task.CompletedTask;
}));
builder.Services.AddHealthChecks();

var app = builder.Build();

// `--migrate [--seed]`: apply migrations (and demo reference data), then exit. Run once per deploy,
// before the API and workers start, so replicas never race on schema changes.
if (args.Contains("--migrate"))
{
    await using var scope = app.Services.CreateAsyncScope();
    var db = scope.ServiceProvider.GetRequiredService<SchedulerDbContext>();
    await db.Database.MigrateAsync();
    if (args.Contains("--seed")) await DemoData.SeedAsync(db);
    app.Logger.LogInformation("Database migrated{Seeded}", args.Contains("--seed") ? " and seeded" : "");
    return;
}

app.UseExceptionHandler();
app.UseStatusCodePages();
app.MapOpenApi();
// Interactive API docs at /swagger, over the built-in OpenAPI document: the client stub for demos and
// manual testing (D-001). Only Swashbuckle's UI is used; the document itself comes from Microsoft.AspNetCore.OpenApi.
app.UseSwaggerUI(o =>
{
    o.SwaggerEndpoint("/openapi/v1.json", "Unified Service Scheduler API v1");
    o.DocumentTitle = "Unified Service Scheduler";
    o.DisplayRequestDuration();
});
app.MapGet("/scalar", () => Results.Redirect("/swagger")).ExcludeFromDescription();   // old demo links
app.MapHealthChecks("/health");
app.MapControllers();

app.Run();

/// <summary>Entry point type, referenced by WebApplicationFactory in the integration tests.</summary>
public partial class Program;
