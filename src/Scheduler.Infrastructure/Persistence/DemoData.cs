using Microsoft.EntityFrameworkCore;

namespace Scheduler.Infrastructure.Persistence;

/// <summary>
/// Fictional reference data for the demo and the compose stack: three dealerships in different time zones,
/// with technicians whose skills differ so that "qualified technician" matters. Safe to run repeatedly.
/// </summary>
public static class DemoData
{
    // ExecuteSqlRaw treats { } as format placeholders, so arrays are written ARRAY['a','b'], not '{a,b}'.
    public static Task SeedAsync(SchedulerDbContext db, CancellationToken ct = default) =>
        db.Database.ExecuteSqlRawAsync("""
            INSERT INTO service_type (code, name, duration_minutes, required_skill) VALUES
              ('OIL',     'Oil and filter change',     60,  'general'),
              ('TYRES',   'Tyre fitting (set of four)', 45, 'tyres'),
              ('BRAKES',  'Brake pad replacement',     90,  'brakes'),
              ('DIAG',    'Diagnostic check',          60,  'diagnostics'),
              ('EV-BATT', 'EV battery health check',   120, 'ev'),
              ('ANNUAL',  'Annual service',            180, 'general')
            ON CONFLICT DO NOTHING;

            INSERT INTO dealership (id, name, time_zone) VALUES
              (1, 'Thames Valley Motors', 'Europe/London'),
              (2, 'Saigon Auto Service',  'Asia/Ho_Chi_Minh'),
              (3, 'Hudson Car Care',      'America/New_York')
            ON CONFLICT DO NOTHING;

            -- Weekday: 0 = Sunday … 6 = Saturday (System.DayOfWeek).
            INSERT INTO opening_hours (dealership_id, weekday, opens, closes)
            SELECT 1, d, time '08:00', time '18:00' FROM generate_series(1, 6) d
            UNION ALL SELECT 2, d, time '07:30', time '17:30' FROM generate_series(1, 6) d
            UNION ALL SELECT 3, d, time '08:00', time '17:00' FROM generate_series(1, 5) d
            UNION ALL SELECT 3, 6, time '09:00', time '13:00'
            ON CONFLICT DO NOTHING;

            INSERT INTO technician (id, dealership_id, name, skills) VALUES
              (101, 1, 'Amelia Hart',    ARRAY['general','brakes','tyres']),
              (102, 1, 'Oliver Grant',   ARRAY['general','diagnostics']),
              (103, 1, 'Priya Shah',     ARRAY['general','ev','diagnostics']),
              (104, 1, 'Tom Whitfield',  ARRAY['tyres']),
              (201, 2, 'Nguyen Van An',  ARRAY['general','brakes']),
              (202, 2, 'Tran Thi Binh',  ARRAY['general','ev','diagnostics']),
              (203, 2, 'Le Minh Chau',   ARRAY['general','tyres']),
              (301, 3, 'Maria Lopez',    ARRAY['general','brakes','diagnostics']),
              (302, 3, 'James Carter',   ARRAY['general','tyres']),
              (303, 3, 'Grace Kim',      ARRAY['ev','diagnostics']),
              (304, 3, 'Daniel Brooks',  ARRAY['general'])
            ON CONFLICT DO NOTHING;

            INSERT INTO bay (id, dealership_id, name) VALUES
              (151, 1, 'Bay 1'), (152, 1, 'Bay 2'), (153, 1, 'Bay 3 (lift)'),
              (251, 2, 'Bay A'), (252, 2, 'Bay B'),
              (351, 3, 'Bay 1'), (352, 3, 'Bay 2'), (353, 3, 'Bay 3'), (354, 3, 'Bay 4')
            ON CONFLICT DO NOTHING;
            """, ct);
}
