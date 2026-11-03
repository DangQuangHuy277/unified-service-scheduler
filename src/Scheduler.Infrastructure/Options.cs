namespace Scheduler.Infrastructure;

public sealed class KafkaOptions
{
    public const string Section = "Kafka";

    public string BootstrapServers { get; set; } = "localhost:9092";

    /// <summary>Booking commands, keyed by dealership id so one dealership always lands on one partition.</summary>
    public string BookingTopic { get; set; } = "booking-commands";

    /// <summary>AppointmentConfirmed events published from the outbox.</summary>
    public string EventsTopic { get; set; } = "appointment-events";

    /// <summary>Upper bound on booking parallelism: one writer per partition.</summary>
    public int Partitions { get; set; } = 16;

    public short ReplicationFactor { get; set; } = 1;
    public string WorkerGroup { get; set; } = "booking-workers";
    public string ProjectorGroup { get; set; } = "availability-projector";

    /// <summary>Create the topics at startup if missing (development, tests, the compose demo).</summary>
    public bool CreateTopics { get; set; } = true;

    /// <summary>How quickly a dead worker's partitions move to another worker.</summary>
    public int SessionTimeoutMs { get; set; } = 10_000;
}

public sealed class BookingOptions
{
    public const string Section = "Booking";

    /// <summary>Most commands one partition decides and writes in a single transaction.</summary>
    public int MaxBatch { get; set; } = 64;

    /// <summary>How long POST /appointments waits for the decision before answering 202 (D-006).</summary>
    public TimeSpan ReplyWait { get; set; } = TimeSpan.FromSeconds(2);

    /// <summary>Upper bound for a client's <c>Prefer: wait=N</c>.</summary>
    public TimeSpan MaxReplyWait { get; set; } = TimeSpan.FromSeconds(10);

    public TimeSpan ReferenceDataRefresh { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>Granularity of the start times /availability offers.</summary>
    public TimeSpan AvailabilityStep { get; set; } = TimeSpan.FromMinutes(15);
}

public sealed class WorkerOptions
{
    public const string Section = "Worker";

    /// <summary>Written to partition_lease.owner, so it shows which process held a partition.</summary>
    public string InstanceId { get; set; } = $"{Environment.MachineName}-{Guid.NewGuid().ToString("N")[..8]}";
    public bool RunBookingWorker { get; set; } = true;
    public bool RunOutboxRelay { get; set; } = true;
    public bool RunProjector { get; set; } = true;
}
