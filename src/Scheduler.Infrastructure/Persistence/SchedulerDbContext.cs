using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design;

namespace Scheduler.Infrastructure.Persistence;

/// <summary>
/// Owns the schema and migrations (D-003). The worker's hot path writes with plain Npgsql commands
/// (Data/Write/BookingStore) so each batch stays one transaction with a handful of statements.
/// </summary>
public sealed class SchedulerDbContext(DbContextOptions<SchedulerDbContext> options) : DbContext(options)
{
    public const string ReadModelSchema = "read";

    public DbSet<DealershipRow> Dealerships => Set<DealershipRow>();
    public DbSet<ServiceTypeRow> ServiceTypes => Set<ServiceTypeRow>();
    public DbSet<TechnicianRow> Technicians => Set<TechnicianRow>();
    public DbSet<BayRow> Bays => Set<BayRow>();
    public DbSet<CustomerRow> Customers => Set<CustomerRow>();
    public DbSet<VehicleRow> Vehicles => Set<VehicleRow>();
    public DbSet<BookingRequestRow> BookingRequests => Set<BookingRequestRow>();
    public DbSet<AppointmentRow> Appointments => Set<AppointmentRow>();
    public DbSet<OutboxMessageRow> OutboxMessages => Set<OutboxMessageRow>();
    public DbSet<PartitionLeaseRow> PartitionLeases => Set<PartitionLeaseRow>();
    public DbSet<AvailabilityBusyRow> AvailabilityBusy => Set<AvailabilityBusyRow>();

    protected override void OnModelCreating(ModelBuilder model)
    {
        // Needed for `technician_id WITH =` inside a GiST exclusion constraint.
        model.HasPostgresExtension("btree_gist");

        model.Entity<DealershipRow>(e =>
        {
            e.ToTable("dealership");
            e.Property(x => x.Id).ValueGeneratedNever();
            e.HasMany(x => x.OpeningHours).WithOne().HasForeignKey(x => x.DealershipId);
            e.HasMany(x => x.Technicians).WithOne().HasForeignKey(x => x.DealershipId);
            e.HasMany(x => x.Bays).WithOne().HasForeignKey(x => x.DealershipId);
        });
        model.Entity<OpeningHoursRow>(e =>
        {
            e.ToTable("opening_hours", t => t.HasCheckConstraint("ck_opening_hours_order", "opens < closes"));
            e.HasKey(x => new { x.DealershipId, x.Weekday });
        });
        model.Entity<ServiceTypeRow>(e =>
        {
            e.ToTable("service_type", t => t.HasCheckConstraint("ck_service_type_duration", "duration_minutes > 0"));
            e.HasKey(x => x.Code);
        });
        model.Entity<TechnicianRow>(e =>
        {
            e.ToTable("technician");
            e.Property(x => x.Id).ValueGeneratedNever();
        });
        model.Entity<BayRow>(e =>
        {
            e.ToTable("bay");
            e.Property(x => x.Id).ValueGeneratedNever();
        });
        model.Entity<CustomerRow>(e =>
        {
            e.ToTable("customer");
            e.HasIndex(x => x.Email).IsUnique();
        });
        model.Entity<VehicleRow>(e =>
        {
            e.ToTable("vehicle");
            e.HasIndex(x => x.Vin).IsUnique();
            e.HasOne<CustomerRow>().WithMany().HasForeignKey(x => x.CustomerId);
        });
        model.Entity<BookingRequestRow>(e =>
        {
            e.ToTable("booking_request");
            e.Property(x => x.Id).ValueGeneratedNever();
        });
        model.Entity<AppointmentRow>(e =>
        {
            // The two exclusion constraints are added as raw SQL in the migration: EF can't express them.
            e.ToTable("appointment");
            e.HasIndex(x => x.BookingRequestId).IsUnique();
            e.HasOne<BookingRequestRow>().WithMany().HasForeignKey(x => x.BookingRequestId);
            e.HasOne<CustomerRow>().WithMany().HasForeignKey(x => x.CustomerId);
            e.HasOne<VehicleRow>().WithMany().HasForeignKey(x => x.VehicleId);
            e.HasOne<ServiceTypeRow>().WithMany().HasForeignKey(x => x.ServiceTypeCode);
            e.HasOne<TechnicianRow>().WithMany().HasForeignKey(x => x.TechnicianId);
            e.HasOne<BayRow>().WithMany().HasForeignKey(x => x.BayId);
            e.HasOne<DealershipRow>().WithMany().HasForeignKey(x => x.DealershipId);
            e.Property(x => x.Slot).HasColumnType("tstzrange");
            e.Property(x => x.CreatedAt).HasDefaultValueSql("now()");
            // Lets the worker load one dealership's future bookings without scanning the table.
            e.HasIndex(x => new { x.DealershipId, x.Slot }).HasMethod("gist");
        });
        model.Entity<OutboxMessageRow>(e =>
        {
            e.ToTable("outbox_message");
            e.Property(x => x.Payload).HasColumnType("jsonb");
            e.Property(x => x.CreatedAt).HasDefaultValueSql("now()");
            e.HasIndex(x => x.Id).HasFilter("published_at IS NULL").HasDatabaseName("ix_outbox_message_unpublished");
        });
        model.Entity<PartitionLeaseRow>(e =>
        {
            e.ToTable("partition_lease");
            e.HasKey(x => new { x.Topic, x.Partition });
        });
        model.Entity<AvailabilityBusyRow>(e =>
        {
            e.ToTable("availability_busy", ReadModelSchema);
            e.HasKey(x => x.AppointmentId);
            e.Property(x => x.AppointmentId).ValueGeneratedNever();
            e.Property(x => x.Slot).HasColumnType("tstzrange");
            e.HasIndex(x => new { x.DealershipId, x.Slot }).HasMethod("gist");
        });
    }
}

/// <summary>Lets <c>dotnet ef</c> build the model without starting a host.</summary>
public sealed class DesignTimeDbContextFactory : IDesignTimeDbContextFactory<SchedulerDbContext>
{
    public SchedulerDbContext CreateDbContext(string[] args) =>
        new(new DbContextOptionsBuilder<SchedulerDbContext>()
            .UseNpgsql("Host=localhost;Database=scheduler_design")
            .UseSnakeCaseNamingConvention()
            .Options);
}
