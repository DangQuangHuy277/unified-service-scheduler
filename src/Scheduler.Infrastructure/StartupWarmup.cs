using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Scheduler.Infrastructure.Messaging;

namespace Scheduler.Infrastructure;

/// <summary>
/// Pays the one-off costs before traffic arrives: EF Core builds its model on the first query, and the Kafka
/// producer connects and fetches topic metadata on first use. Without this the first booking after a deploy
/// took ~2.5 s and fell past the API's 2 s reply window into a 202. Failures are logged, not fatal: everything
/// still initialises lazily on first use.
/// </summary>
public sealed class StartupWarmup(
    ReferenceDataCache reference,
    KafkaProducer kafka,
    IOptions<KafkaOptions> options,
    ILogger<StartupWarmup> log) : IHostedService
{
    public async Task StartAsync(CancellationToken ct)
    {
        try
        {
            await reference.GetAsync(ct);
            using var admin = new DependentAdminClientBuilder(kafka.Producer.Handle).Build();
            admin.GetMetadata(options.Value.BookingTopic, TimeSpan.FromSeconds(10));
        }
        catch (Exception e) when (!ct.IsCancellationRequested)
        {
            log.LogWarning(e, "Warm-up failed; continuing, the first requests will be slower");
        }
    }

    public Task StopAsync(CancellationToken ct) => Task.CompletedTask;
}
