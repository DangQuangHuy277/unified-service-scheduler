using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Text;
using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Npgsql;
using OpenTelemetry;
using OpenTelemetry.Logs;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;

namespace Scheduler.Infrastructure;

/// <summary>One ActivitySource and one Meter for everything the service itself measures.</summary>
public static class SchedulerTelemetry
{
    public const string Name = "Scheduler";

    public static readonly ActivitySource Source = new(Name);
    public static readonly Meter Meter = new(Name);

    public static readonly Counter<long> Decisions = Meter.CreateCounter<long>(
        "scheduler.booking.decisions", description: "Booking requests decided by a worker, tagged by outcome.");

    public static readonly Histogram<double> DecisionLatency = Meter.CreateHistogram<double>(
        "scheduler.booking.decision_latency", "ms", "From the API enqueuing a command to its batch committing.");

    public static readonly Histogram<int> BatchSize = Meter.CreateHistogram<int>(
        "scheduler.booking.batch_size", description: "Commands decided per transaction.");

    public static readonly Histogram<double> ScheduleLoad = Meter.CreateHistogram<double>(
        "scheduler.booking.schedule_load", "ms", "Loading one dealership's schedule into a worker (cold start).");

    public static readonly Counter<long> FenceRejections = Meter.CreateCounter<long>(
        "scheduler.booking.fence_rejections", description: "Batches refused because another worker now owns the partition.");

    public static readonly Counter<long> BackstopViolations = Meter.CreateCounter<long>(
        "scheduler.booking.backstop_violations", description: "Exclusion-constraint violations (23P01). Should stay 0.");

    public static readonly Counter<long> ReplyTimeouts = Meter.CreateCounter<long>(
        "scheduler.api.reply_timeouts", description: "POST /appointments answered 202 because no decision arrived in time.");

    public static readonly Histogram<double> ProjectionLag = Meter.CreateHistogram<double>(
        "scheduler.projection.lag", "ms", "From confirmation to the availability read model reflecting it.");

    private static readonly ConcurrentDictionary<int, long> ConsumerLagByPartition = new();
    private static long _outboxBacklog;

    static SchedulerTelemetry()
    {
        Meter.CreateObservableGauge("scheduler.booking.consumer_lag",
            () => ConsumerLagByPartition.Select(p => new Measurement<long>(p.Value, new KeyValuePair<string, object?>("partition", p.Key))),
            description: "Booking commands waiting in Kafka per owned partition.");
        Meter.CreateObservableGauge("scheduler.outbox.backlog", () => Interlocked.Read(ref _outboxBacklog),
            description: "Outbox events not yet published.");
    }

    public static void ReportConsumerLag(int partition, long lag) => ConsumerLagByPartition[partition] = lag;
    public static void ForgetConsumerLag(int partition) => ConsumerLagByPartition.TryRemove(partition, out _);
    public static void ReportOutboxBacklog(long backlog) => Interlocked.Exchange(ref _outboxBacklog, backlog);

    /// <summary>Writes the current W3C trace context into Kafka headers so the consumer can continue the trace.</summary>
    public static void InjectTraceContext(Headers headers)
    {
        if (Activity.Current is not { } activity) return;
        headers.Add("traceparent", Encoding.UTF8.GetBytes(activity.Id!));
        if (activity.TraceStateString is { } state) headers.Add("tracestate", Encoding.UTF8.GetBytes(state));
    }

    public static ActivityContext ExtractTraceContext(Headers? headers)
    {
        if (headers is null || !headers.TryGetLastBytes("traceparent", out var parent)) return default;
        var state = headers.TryGetLastBytes("tracestate", out var s) ? Encoding.UTF8.GetString(s) : null;
        return ActivityContext.TryParse(Encoding.UTF8.GetString(parent), state, out var context) ? context : default;
    }

    /// <summary>
    /// Traces, metrics and logs through OpenTelemetry. Exported over OTLP only when
    /// OTEL_EXPORTER_OTLP_ENDPOINT is set (the compose file points it at the Aspire dashboard).
    /// </summary>
    public static IHostApplicationBuilder AddSchedulerTelemetry(this IHostApplicationBuilder builder, string serviceName,
        Action<TracerProviderBuilder>? tracing = null, Action<MeterProviderBuilder>? metrics = null)
    {
        var otel = builder.Services.AddOpenTelemetry()
            .ConfigureResource(r => r.AddService(serviceName))
            .WithTracing(t =>
            {
                t.AddSource(Name).AddNpgsql();
                tracing?.Invoke(t);
            })
            .WithMetrics(m =>
            {
                m.AddMeter(Name, "Npgsql").AddRuntimeInstrumentation();
                metrics?.Invoke(m);
            });
        builder.Logging.AddOpenTelemetry(o =>
        {
            o.IncludeFormattedMessage = true;
            o.IncludeScopes = true;
        });
        if (!string.IsNullOrWhiteSpace(builder.Configuration["OTEL_EXPORTER_OTLP_ENDPOINT"])) otel.UseOtlpExporter();
        return builder;
    }
}
