using Npgsql;

namespace BookingConcurrency;

/// <summary>Schema and seed data. Mirrors the design: two GiST exclusion constraints guard every write.</summary>
public static class Database
{
    public static async Task CreateSchemaAsync(NpgsqlDataSource db, World world)
    {
        await ExecAsync(db, """
            CREATE EXTENSION IF NOT EXISTS btree_gist;
            DROP TABLE IF EXISTS appointment, technician, bay, dealership;
            CREATE TABLE dealership (id int PRIMARY KEY);
            CREATE TABLE technician (id int PRIMARY KEY, dealership_id int NOT NULL REFERENCES dealership);
            CREATE TABLE bay        (id int PRIMARY KEY, dealership_id int NOT NULL REFERENCES dealership);
            CREATE INDEX ON technician (dealership_id, id);
            CREATE INDEX ON bay (dealership_id, id);
            CREATE TABLE appointment (
                id            bigserial PRIMARY KEY,
                dealership_id int NOT NULL,
                technician_id int NOT NULL,
                bay_id        int NOT NULL,
                slot          tstzrange NOT NULL,
                CONSTRAINT no_technician_overlap EXCLUDE USING gist (technician_id WITH =, slot WITH &&),
                CONSTRAINT no_bay_overlap        EXCLUDE USING gist (bay_id WITH =, slot WITH &&)
            );
            CREATE INDEX ON appointment USING gist (dealership_id, slot);
            """);
        await ExecAsync(db, $"""
            INSERT INTO dealership SELECT d FROM generate_series(1, {world.Dealerships}) d;
            INSERT INTO technician SELECT d * 1000 + n, d
              FROM generate_series(1, {world.Dealerships}) d, generate_series(1, {world.TechniciansPerDealership}) n;
            INSERT INTO bay SELECT d * 1000 + n, d
              FROM generate_series(1, {world.Dealerships}) d, generate_series(1, {world.BaysPerDealership}) n;
            ANALYZE dealership, technician, bay;
            """);
    }

    /// <summary>
    /// Fills past days half full once per world, so the planner sees a realistic table (and uses
    /// the GiST indexes) instead of an empty one.
    /// </summary>
    public static async Task SeedHistoryAsync(NpgsqlDataSource db, World world)
    {
        var halfCapacity = Math.Max(1, Math.Min(world.TechniciansPerDealership, world.BaysPerDealership) / 2);
        await ExecAsync(db, $"""
            INSERT INTO appointment (dealership_id, technician_id, bay_id, slot)
            SELECT d, d * 1000 + n, d * 1000 + n,
                   tstzrange(ts, ts + interval '1 hour', '[)')
              FROM generate_series(1, {world.Dealerships}) d,
                   generate_series(1, {halfCapacity}) n,
                   generate_series(-{world.HistoryDays}, -1) day,
                   generate_series(8, 17) h,
                   LATERAL (SELECT timestamptz '{World.Day0:yyyy-MM-dd} 00:00Z'
                                   + make_interval(days => day, hours => h) AS ts) t
            """);
    }

    /// <summary>
    /// Removes the previous run's bookings and leaves the server quiet: dead rows vacuumed, stats
    /// fresh, dirty pages flushed, so no checkpoint or autovacuum lands inside the measured run.
    /// </summary>
    public static async Task ResetAppointmentsAsync(NpgsqlDataSource db)
    {
        await ExecAsync(db, $"DELETE FROM appointment WHERE lower(slot) >= timestamptz '{World.Day0:yyyy-MM-dd} 00:00Z'");
        await ExecAsync(db, "VACUUM (ANALYZE) appointment");
        await ExecAsync(db, "CHECKPOINT");
    }

    /// <summary>Rows written by the run (on or after day 0) and any overlap that slipped through.</summary>
    public static async Task<(long Rows, long TechOverlaps, long BayOverlaps)> VerifyAsync(NpgsqlDataSource db)
    {
        await using var cmd = db.CreateCommand($"""
            WITH run AS (SELECT * FROM appointment WHERE lower(slot) >= timestamptz '{World.Day0:yyyy-MM-dd} 00:00Z')
            SELECT (SELECT count(*) FROM run),
                   (SELECT count(*) FROM run a JOIN run b
                      ON a.id < b.id AND a.technician_id = b.technician_id AND a.slot && b.slot),
                   (SELECT count(*) FROM run a JOIN run b
                      ON a.id < b.id AND a.bay_id = b.bay_id AND a.slot && b.slot)
            """);
        await using var reader = await cmd.ExecuteReaderAsync();
        await reader.ReadAsync();
        return (reader.GetInt64(0), reader.GetInt64(1), reader.GetInt64(2));
    }

    private static async Task ExecAsync(NpgsqlDataSource db, string sql)
    {
        await using var cmd = db.CreateCommand(sql);
        cmd.CommandTimeout = 300;
        await cmd.ExecuteNonQueryAsync();
    }
}
