using System.Diagnostics;
using Confluent.Kafka;
using Microsoft.Extensions.Options;
using Scheduler.Infrastructure;
using Scheduler.Infrastructure.Messaging;

namespace Scheduler.Api.Booking;

/// <summary>
/// Puts a command on the booking topic, keyed by dealership so the same worker always decides for it.
/// This is the API's only write on the request path: no database commit per request (D-005).
/// </summary>
public sealed class BookingCommandPublisher(KafkaProducer kafka, IOptions<KafkaOptions> options)
{
    public async Task PublishAsync(BookAppointmentCommand command)
    {
        var topic = options.Value.BookingTopic;
        using var activity = SchedulerTelemetry.Source.StartActivity($"{topic} publish", ActivityKind.Producer);
        activity?.SetTag("messaging.system", "kafka");
        activity?.SetTag("messaging.destination.name", topic);
        activity?.SetTag("scheduler.request_id", command.RequestId);
        activity?.SetTag("scheduler.dealership_id", command.DealershipId);

        var headers = new Headers();
        SchedulerTelemetry.InjectTraceContext(headers);
        // Not tied to the HTTP request's cancellation: once handed to Kafka the booking proceeds, and the
        // client can still find the outcome at /booking-requests/{id}.
        var delivery = await kafka.Producer.ProduceAsync(topic, new Message<string, byte[]>
        {
            Key = command.DealershipId.ToString(),
            Value = MessageJson.Serialize(command),
            Headers = headers,
            Timestamp = new Timestamp(command.RequestedAt),
        });
        activity?.SetTag("messaging.destination.partition.id", delivery.Partition.Value);
    }
}
