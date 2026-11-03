using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Npgsql;
using Scheduler.Infrastructure.Data;
using Scheduler.Infrastructure.Messaging;
using Scheduler.Infrastructure.Persistence;

namespace Scheduler.Infrastructure;

public static class ServiceRegistration
{
    /// <summary>Postgres (one data source shared by EF Core and raw commands), reference data and options.</summary>
    public static IHostApplicationBuilder AddSchedulerData(this IHostApplicationBuilder builder)
    {
        var services = builder.Services;
        services.AddOptions<KafkaOptions>().BindConfiguration(KafkaOptions.Section);
        services.AddOptions<BookingOptions>().BindConfiguration(BookingOptions.Section);
        services.AddSingleton(TimeProvider.System);

        // Resolved lazily so test hosts can supply the connection string through configuration overrides.
        services.AddSingleton(sp =>
        {
            var connectionString = sp.GetRequiredService<IConfiguration>().GetConnectionString("Scheduler")
                ?? throw new InvalidOperationException("ConnectionStrings:Scheduler is not configured.");
            var dataSource = new NpgsqlDataSourceBuilder(connectionString);
            // Trace a database call only as part of a larger operation (an HTTP request, a booking batch).
            // Without this, the outbox relay's idle 1 s poll and the LISTEN loops each start a one-span trace
            // every second, which buries the traces that matter.
            dataSource.ConfigureTracing(t => t
                .ConfigureCommandFilter(_ => Activity.Current is not null)
                .ConfigureBatchFilter(_ => Activity.Current is not null));
            return dataSource.Build();
        });
        services.AddDbContext<SchedulerDbContext>((sp, o) => o
            .UseNpgsql(sp.GetRequiredService<NpgsqlDataSource>())
            .UseSnakeCaseNamingConvention());
        services.AddSingleton<ReferenceDataCache>();

        // Data access (Data/): every query in the solution lives behind one of these. Stateless, so singletons.
        services.AddSingleton<ReferenceDataStore>();
        services.AddSingleton<ReadModelStore>();
        services.AddSingleton<LeaseStore>();
        services.AddSingleton<BookingStore>();
        services.AddSingleton<OutboxStore>();
        services.AddSingleton<BookingReadQueries>();
        return builder;
    }

    public static IHostApplicationBuilder AddSchedulerKafka(this IHostApplicationBuilder builder)
    {
        // Hosted services start in registration order: topics exist before the warm-up looks them up.
        builder.Services.AddHostedService<KafkaTopicsInitializer>();
        builder.Services.AddSingleton<KafkaProducer>();
        builder.Services.AddHostedService<StartupWarmup>();
        return builder;
    }
}
