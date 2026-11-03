using Scheduler.Infrastructure;
using Scheduler.Worker.Booking;
using Scheduler.Worker.ReadSide;

namespace Scheduler.Worker;

/// <summary>
/// The worker process: booking writer (one owner per Kafka partition), outbox relay and availability
/// projector. Each role can be switched off, so they can also be scaled as separate deployments.
/// </summary>
public static class WorkerProgram
{
    public static async Task Main(string[] args) => await CreateBuilder(args).Build().RunAsync();

    /// <param name="configure">Applied before services are registered; tests use it to override configuration.</param>
    public static HostApplicationBuilder CreateBuilder(string[] args, Action<HostApplicationBuilder>? configure = null)
    {
        var builder = Host.CreateApplicationBuilder(args);
        configure?.Invoke(builder);

        builder.AddSchedulerData().AddSchedulerKafka().AddSchedulerTelemetry("scheduler-worker");
        builder.Services.AddOptions<WorkerOptions>().BindConfiguration(WorkerOptions.Section);
        builder.Services.AddSingleton<PartitionOwnership>();
        builder.Services.AddSingleton<PartitionLeaseStore>();
        builder.Services.AddSingleton<BookingBatchProcessor>();

        var roles = builder.Configuration.GetSection(WorkerOptions.Section).Get<WorkerOptions>() ?? new WorkerOptions();
        if (roles.RunBookingWorker) builder.Services.AddHostedService<BookingWorker>();
        if (roles.RunOutboxRelay) builder.Services.AddHostedService<OutboxRelay>();
        if (roles.RunProjector) builder.Services.AddHostedService<AvailabilityProjector>();
        return builder;
    }
}
