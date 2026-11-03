using Confluent.Kafka;
using Microsoft.Extensions.Options;
using Scheduler.Infrastructure;
using Scheduler.Infrastructure.Data;
using Scheduler.Infrastructure.Messaging;

namespace Scheduler.Worker.Booking;

/// <summary>
/// The single writer. Kafka's consumer group gives each partition (and so each dealership) exactly one owner;
/// on assignment the worker takes a fencing epoch, then drains each partition in batches through
/// <see cref="BookingBatchProcessor"/>. Offsets are stored only after a batch commits, so a crash replays
/// the batch and the processor's dedupe makes the replay harmless.
/// </summary>
public sealed class BookingWorker(
    BookingBatchProcessor processor,
    LeaseStore leases,
    PartitionOwnership ownership,
    IOptions<KafkaOptions> kafkaOptions,
    IOptions<BookingOptions> bookingOptions,
    IOptions<WorkerOptions> workerOptions,
    ILogger<BookingWorker> log) : BackgroundService
{
    private readonly KafkaOptions _kafka = kafkaOptions.Value;
    private readonly int _maxBatch = bookingOptions.Value.MaxBatch;
    private readonly string _instance = workerOptions.Value.InstanceId;

    // Confluent consumers are not thread-safe and their handlers run inside Consume(), so the whole
    // loop runs on one dedicated thread.
    protected override Task ExecuteAsync(CancellationToken ct) =>
        Task.Factory.StartNew(() => Run(ct), ct, TaskCreationOptions.LongRunning, TaskScheduler.Default);

    private void Run(CancellationToken ct)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = _kafka.BootstrapServers,
            GroupId = _kafka.WorkerGroup,
            ClientId = _instance,
            EnableAutoCommit = false,
            EnableAutoOffsetStore = false,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            PartitionAssignmentStrategy = PartitionAssignmentStrategy.CooperativeSticky,
            SessionTimeoutMs = _kafka.SessionTimeoutMs,
            HeartbeatIntervalMs = Math.Max(1000, _kafka.SessionTimeoutMs / 4),
        };
        using var consumer = new ConsumerBuilder<string, byte[]>(config)
            .SetPartitionsAssignedHandler((_, assigned) => OnAssigned(assigned, ct))
            .SetPartitionsRevokedHandler((_, revoked) => OnReleased(revoked.Select(p => p.Partition.Value), "revoked"))
            .SetPartitionsLostHandler((_, lost) => OnReleased(lost.Select(p => p.Partition.Value), "lost"))
            .SetErrorHandler((_, e) => log.LogWarning("Kafka consumer error: {Reason}", e.Reason))
            .Build();
        consumer.Subscribe(_kafka.BookingTopic);
        log.LogInformation("Booking worker {Instance} joined group {Group}", _instance, _kafka.WorkerGroup);

        try
        {
            while (!ct.IsCancellationRequested)
            {
                var first = consumer.Consume(ct);
                var polled = new List<ConsumeResult<string, byte[]>> { first };
                // Drain what is already fetched, without waiting: this is what turns a burst into one batch.
                while (polled.Count < _maxBatch * 4 && consumer.Consume(TimeSpan.Zero) is { } next) polled.Add(next);

                foreach (var partition in polled.GroupBy(r => r.Partition.Value))
                    ProcessPartition(consumer, partition.Key, partition.ToList(), ct);

                CommitStoredOffsets(consumer);
                ReportLag(consumer);
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
        }
        finally
        {
            // Leaving the group cleanly hands the partitions over now instead of after the session timeout.
            consumer.Close();
        }
    }

    private void ProcessPartition(IConsumer<string, byte[]> consumer, int partition,
        List<ConsumeResult<string, byte[]>> results, CancellationToken ct)
    {
        foreach (var chunk in results.Chunk(_maxBatch))
        {
            // Revoked while these messages were in flight: the new owner will read them again.
            if (!ownership.TryGet(partition, out var state)) return;

            var envelopes = new List<CommandEnvelope>(chunk.Length);
            foreach (var result in chunk)
            {
                try
                {
                    envelopes.Add(new CommandEnvelope(
                        MessageJson.Deserialize<BookAppointmentCommand>(result.Message.Value),
                        result.Message.Timestamp.UtcDateTime,
                        SchedulerTelemetry.ExtractTraceContext(result.Message.Headers)));
                }
                catch (Exception e)
                {
                    // Retrying a malformed message would block the partition forever; skip and record it.
                    log.LogError(e, "Skipping malformed booking command at {Offset}", result.TopicPartitionOffset);
                }
            }

            try
            {
                if (envelopes.Count > 0) processor.ProcessAsync(state, envelopes, ct).GetAwaiter().GetResult();
                consumer.StoreOffset(chunk[^1]);
            }
            catch (PartitionLostException e)
            {
                SchedulerTelemetry.FenceRejections.Add(1);
                log.LogWarning(e, "Fenced off partition {Partition}; leaving it to the new owner", partition);
                ownership.Remove(partition);
                return;
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception e)
            {
                // Rewind and try this chunk again on the next poll; schedules were already invalidated.
                log.LogError(e, "Batch failed on partition {Partition}; retrying from offset {Offset}", partition, chunk[0].Offset);
                consumer.Seek(chunk[0].TopicPartitionOffset);
                ct.WaitHandle.WaitOne(TimeSpan.FromMilliseconds(500));
                return;
            }
        }
    }

    private void OnAssigned(List<TopicPartition> assigned, CancellationToken ct)
    {
        foreach (var tp in assigned)
        {
            while (!ct.IsCancellationRequested)
            {
                try
                {
                    var epoch = leases.AcquireAsync(_kafka.BookingTopic, tp.Partition.Value, _instance, ct).GetAwaiter().GetResult();
                    ownership.Set(new PartitionState(tp.Partition.Value, epoch));
                    log.LogInformation("Owns partition {Partition} at epoch {Epoch}", tp.Partition.Value, epoch);
                    break;
                }
                catch (Exception e) when (!ct.IsCancellationRequested)
                {
                    log.LogWarning(e, "Could not take the lease for partition {Partition}; retrying", tp.Partition.Value);
                    ct.WaitHandle.WaitOne(TimeSpan.FromSeconds(1));
                }
            }
        }
    }

    private void OnReleased(IEnumerable<int> partitions, string how)
    {
        foreach (var partition in partitions)
        {
            ownership.Remove(partition);
            SchedulerTelemetry.ForgetConsumerLag(partition);
            log.LogInformation("Partition {Partition} {How}", partition, how);
        }
    }

    private void CommitStoredOffsets(IConsumer<string, byte[]> consumer)
    {
        try
        {
            consumer.Commit();
        }
        catch (KafkaException e) when (e.Error.Code == ErrorCode.Local_NoOffset)
        {
            // Nothing new was stored (for example the batch failed and was rewound).
        }
        catch (KafkaException e)
        {
            // The batch is already in the database; a replay after this is deduplicated.
            log.LogWarning(e, "Offset commit failed");
        }
    }

    private static void ReportLag(IConsumer<string, byte[]> consumer)
    {
        foreach (var tp in consumer.Assignment)
        {
            var position = consumer.Position(tp);
            var high = consumer.GetWatermarkOffsets(tp).High;
            if (position.Value >= 0 && high.Value >= 0)
                SchedulerTelemetry.ReportConsumerLag(tp.Partition.Value, Math.Max(0, high.Value - position.Value));
        }
    }
}
