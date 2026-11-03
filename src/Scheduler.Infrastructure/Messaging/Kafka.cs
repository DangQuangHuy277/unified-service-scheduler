using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Scheduler.Infrastructure.Messaging;

/// <summary>Creates the two topics if they don't exist yet. Runs before the other hosted services start.</summary>
public sealed class KafkaTopicsInitializer(IOptions<KafkaOptions> options, ILogger<KafkaTopicsInitializer> log) : IHostedService
{
    public async Task StartAsync(CancellationToken ct)
    {
        var o = options.Value;
        if (!o.CreateTopics) return;

        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = o.BootstrapServers }).Build();
        var topics = new[] { o.BookingTopic, o.EventsTopic }.Select(name => new TopicSpecification
        {
            Name = name,
            NumPartitions = o.Partitions,
            ReplicationFactor = o.ReplicationFactor,
        });
        try
        {
            await admin.CreateTopicsAsync(topics);
            log.LogInformation("Created topics {Booking} and {Events} with {Partitions} partitions", o.BookingTopic, o.EventsTopic, o.Partitions);
        }
        catch (CreateTopicsException e) when (e.Results.All(r => r.Error.Code is ErrorCode.NoError or ErrorCode.TopicAlreadyExists))
        {
            // Another instance got there first.
        }
    }

    public Task StopAsync(CancellationToken ct) => Task.CompletedTask;
}

/// <summary>One shared producer per process (Confluent producers are thread-safe and batch internally).</summary>
public sealed class KafkaProducer(IOptions<KafkaOptions> options) : IDisposable
{
    public IProducer<string, byte[]> Producer { get; } = new ProducerBuilder<string, byte[]>(new ProducerConfig
    {
        BootstrapServers = options.Value.BootstrapServers,
        Acks = Acks.All,
        EnableIdempotence = true,
        LingerMs = 2,
    }).Build();

    public void Dispose()
    {
        Producer.Flush(TimeSpan.FromSeconds(5));
        Producer.Dispose();
    }
}
