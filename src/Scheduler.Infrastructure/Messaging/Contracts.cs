using System.Text.Json;

namespace Scheduler.Infrastructure.Messaging;

public sealed record CustomerDetails(string Name, string Email);

public sealed record VehicleDetails(string Vin, string Make, string Model);

/// <summary>
/// Published by the API to the booking topic, keyed by <see cref="DealershipId"/>. Already validated against
/// cached reference data; the owning worker re-checks everything before deciding.
/// </summary>
public sealed record BookAppointmentCommand(
    Guid RequestId,
    int DealershipId,
    string ServiceTypeCode,
    DateTime Start,
    CustomerDetails Customer,
    VehicleDetails Vehicle,
    DateTime RequestedAt);

/// <summary>Written to the outbox with the appointment; published to the events topic by the relay.</summary>
public sealed record AppointmentConfirmedEvent(
    long AppointmentId,
    Guid RequestId,
    int DealershipId,
    string ServiceTypeCode,
    int TechnicianId,
    int BayId,
    DateTime Start,
    DateTime End,
    DateTime ConfirmedAt);

public static class MessageJson
{
    public const string AppointmentConfirmed = "AppointmentConfirmed";

    public static readonly JsonSerializerOptions Options = new(JsonSerializerDefaults.Web);

    public static byte[] Serialize<T>(T message) => JsonSerializer.SerializeToUtf8Bytes(message, Options);

    public static string SerializeToString<T>(T message) => JsonSerializer.Serialize(message, Options);

    public static T Deserialize<T>(ReadOnlySpan<byte> utf8) =>
        JsonSerializer.Deserialize<T>(utf8, Options) ?? throw new JsonException($"Empty {typeof(T).Name} message.");
}
