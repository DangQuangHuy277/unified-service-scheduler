using System;
using Microsoft.EntityFrameworkCore.Migrations;
using Npgsql.EntityFrameworkCore.PostgreSQL.Metadata;
using NpgsqlTypes;

#nullable disable

namespace Scheduler.Infrastructure.Persistence.Migrations
{
    /// <inheritdoc />
    public partial class InitialSchema : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.EnsureSchema(
                name: "read");

            migrationBuilder.AlterDatabase()
                .Annotation("Npgsql:PostgresExtension:btree_gist", ",,");

            migrationBuilder.CreateTable(
                name: "availability_busy",
                schema: "read",
                columns: table => new
                {
                    appointment_id = table.Column<long>(type: "bigint", nullable: false),
                    dealership_id = table.Column<int>(type: "integer", nullable: false),
                    technician_id = table.Column<int>(type: "integer", nullable: false),
                    bay_id = table.Column<int>(type: "integer", nullable: false),
                    slot = table.Column<NpgsqlRange<DateTime>>(type: "tstzrange", nullable: false),
                    confirmed_at = table.Column<DateTime>(type: "timestamp with time zone", nullable: false),
                    projected_at = table.Column<DateTime>(type: "timestamp with time zone", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("pk_availability_busy", x => x.appointment_id);
                });

            migrationBuilder.CreateTable(
                name: "booking_request",
                columns: table => new
                {
                    id = table.Column<Guid>(type: "uuid", nullable: false),
                    dealership_id = table.Column<int>(type: "integer", nullable: false),
                    service_type_code = table.Column<string>(type: "text", nullable: false),
                    requested_start = table.Column<DateTime>(type: "timestamp with time zone", nullable: false),
                    outcome = table.Column<string>(type: "text", nullable: false),
                    received_at = table.Column<DateTime>(type: "timestamp with time zone", nullable: false),
                    decided_at = table.Column<DateTime>(type: "timestamp with time zone", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("pk_booking_request", x => x.id);
                });

            migrationBuilder.CreateTable(
                name: "customer",
                columns: table => new
                {
                    id = table.Column<long>(type: "bigint", nullable: false)
                        .Annotation("Npgsql:ValueGenerationStrategy", NpgsqlValueGenerationStrategy.IdentityByDefaultColumn),
                    email = table.Column<string>(type: "text", nullable: false),
                    name = table.Column<string>(type: "text", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("pk_customer", x => x.id);
                });

            migrationBuilder.CreateTable(
                name: "dealership",
                columns: table => new
                {
                    id = table.Column<int>(type: "integer", nullable: false),
                    name = table.Column<string>(type: "text", nullable: false),
                    time_zone = table.Column<string>(type: "text", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("pk_dealership", x => x.id);
                });

            migrationBuilder.CreateTable(
                name: "outbox_message",
                columns: table => new
                {
                    id = table.Column<long>(type: "bigint", nullable: false)
                        .Annotation("Npgsql:ValueGenerationStrategy", NpgsqlValueGenerationStrategy.IdentityByDefaultColumn),
                    topic = table.Column<string>(type: "text", nullable: false),
                    key = table.Column<string>(type: "text", nullable: false),
                    type = table.Column<string>(type: "text", nullable: false),
                    payload = table.Column<string>(type: "jsonb", nullable: false),
                    created_at = table.Column<DateTime>(type: "timestamp with time zone", nullable: false, defaultValueSql: "now()"),
                    published_at = table.Column<DateTime>(type: "timestamp with time zone", nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("pk_outbox_message", x => x.id);
                });

            migrationBuilder.CreateTable(
                name: "partition_lease",
                columns: table => new
                {
                    topic = table.Column<string>(type: "text", nullable: false),
                    partition = table.Column<int>(type: "integer", nullable: false),
                    epoch = table.Column<long>(type: "bigint", nullable: false),
                    owner = table.Column<string>(type: "text", nullable: false),
                    acquired_at = table.Column<DateTime>(type: "timestamp with time zone", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("pk_partition_lease", x => new { x.topic, x.partition });
                });

            migrationBuilder.CreateTable(
                name: "service_type",
                columns: table => new
                {
                    code = table.Column<string>(type: "text", nullable: false),
                    name = table.Column<string>(type: "text", nullable: false),
                    duration_minutes = table.Column<int>(type: "integer", nullable: false),
                    required_skill = table.Column<string>(type: "text", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("pk_service_type", x => x.code);
                    table.CheckConstraint("ck_service_type_duration", "duration_minutes > 0");
                });

            migrationBuilder.CreateTable(
                name: "vehicle",
                columns: table => new
                {
                    id = table.Column<long>(type: "bigint", nullable: false)
                        .Annotation("Npgsql:ValueGenerationStrategy", NpgsqlValueGenerationStrategy.IdentityByDefaultColumn),
                    vin = table.Column<string>(type: "text", nullable: false),
                    make = table.Column<string>(type: "text", nullable: false),
                    model = table.Column<string>(type: "text", nullable: false),
                    customer_id = table.Column<long>(type: "bigint", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("pk_vehicle", x => x.id);
                    table.ForeignKey(
                        name: "fk_vehicle_customer_customer_id",
                        column: x => x.customer_id,
                        principalTable: "customer",
                        principalColumn: "id",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "bay",
                columns: table => new
                {
                    id = table.Column<int>(type: "integer", nullable: false),
                    dealership_id = table.Column<int>(type: "integer", nullable: false),
                    name = table.Column<string>(type: "text", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("pk_bay", x => x.id);
                    table.ForeignKey(
                        name: "fk_bay_dealership_dealership_id",
                        column: x => x.dealership_id,
                        principalTable: "dealership",
                        principalColumn: "id",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "opening_hours",
                columns: table => new
                {
                    dealership_id = table.Column<int>(type: "integer", nullable: false),
                    weekday = table.Column<int>(type: "integer", nullable: false),
                    opens = table.Column<TimeOnly>(type: "time without time zone", nullable: false),
                    closes = table.Column<TimeOnly>(type: "time without time zone", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("pk_opening_hours", x => new { x.dealership_id, x.weekday });
                    table.CheckConstraint("ck_opening_hours_order", "opens < closes");
                    table.ForeignKey(
                        name: "fk_opening_hours_dealership_dealership_id",
                        column: x => x.dealership_id,
                        principalTable: "dealership",
                        principalColumn: "id",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "technician",
                columns: table => new
                {
                    id = table.Column<int>(type: "integer", nullable: false),
                    dealership_id = table.Column<int>(type: "integer", nullable: false),
                    name = table.Column<string>(type: "text", nullable: false),
                    skills = table.Column<string[]>(type: "text[]", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("pk_technician", x => x.id);
                    table.ForeignKey(
                        name: "fk_technician_dealership_dealership_id",
                        column: x => x.dealership_id,
                        principalTable: "dealership",
                        principalColumn: "id",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "appointment",
                columns: table => new
                {
                    id = table.Column<long>(type: "bigint", nullable: false)
                        .Annotation("Npgsql:ValueGenerationStrategy", NpgsqlValueGenerationStrategy.IdentityByDefaultColumn),
                    booking_request_id = table.Column<Guid>(type: "uuid", nullable: false),
                    dealership_id = table.Column<int>(type: "integer", nullable: false),
                    customer_id = table.Column<long>(type: "bigint", nullable: false),
                    vehicle_id = table.Column<long>(type: "bigint", nullable: false),
                    service_type_code = table.Column<string>(type: "text", nullable: false),
                    technician_id = table.Column<int>(type: "integer", nullable: false),
                    bay_id = table.Column<int>(type: "integer", nullable: false),
                    slot = table.Column<NpgsqlRange<DateTime>>(type: "tstzrange", nullable: false),
                    created_at = table.Column<DateTime>(type: "timestamp with time zone", nullable: false, defaultValueSql: "now()")
                },
                constraints: table =>
                {
                    table.PrimaryKey("pk_appointment", x => x.id);
                    table.ForeignKey(
                        name: "fk_appointment_bay_bay_id",
                        column: x => x.bay_id,
                        principalTable: "bay",
                        principalColumn: "id",
                        onDelete: ReferentialAction.Cascade);
                    table.ForeignKey(
                        name: "fk_appointment_booking_request_booking_request_id",
                        column: x => x.booking_request_id,
                        principalTable: "booking_request",
                        principalColumn: "id",
                        onDelete: ReferentialAction.Cascade);
                    table.ForeignKey(
                        name: "fk_appointment_customer_customer_id",
                        column: x => x.customer_id,
                        principalTable: "customer",
                        principalColumn: "id",
                        onDelete: ReferentialAction.Cascade);
                    table.ForeignKey(
                        name: "fk_appointment_dealership_dealership_id",
                        column: x => x.dealership_id,
                        principalTable: "dealership",
                        principalColumn: "id",
                        onDelete: ReferentialAction.Cascade);
                    table.ForeignKey(
                        name: "fk_appointment_service_type_service_type_code",
                        column: x => x.service_type_code,
                        principalTable: "service_type",
                        principalColumn: "code",
                        onDelete: ReferentialAction.Cascade);
                    table.ForeignKey(
                        name: "fk_appointment_technician_technician_id",
                        column: x => x.technician_id,
                        principalTable: "technician",
                        principalColumn: "id",
                        onDelete: ReferentialAction.Cascade);
                    table.ForeignKey(
                        name: "fk_appointment_vehicle_vehicle_id",
                        column: x => x.vehicle_id,
                        principalTable: "vehicle",
                        principalColumn: "id",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateIndex(
                name: "ix_appointment_bay_id",
                table: "appointment",
                column: "bay_id");

            migrationBuilder.CreateIndex(
                name: "ix_appointment_booking_request_id",
                table: "appointment",
                column: "booking_request_id",
                unique: true);

            migrationBuilder.CreateIndex(
                name: "ix_appointment_customer_id",
                table: "appointment",
                column: "customer_id");

            migrationBuilder.CreateIndex(
                name: "ix_appointment_dealership_id_slot",
                table: "appointment",
                columns: new[] { "dealership_id", "slot" })
                .Annotation("Npgsql:IndexMethod", "gist");

            migrationBuilder.CreateIndex(
                name: "ix_appointment_service_type_code",
                table: "appointment",
                column: "service_type_code");

            migrationBuilder.CreateIndex(
                name: "ix_appointment_technician_id",
                table: "appointment",
                column: "technician_id");

            migrationBuilder.CreateIndex(
                name: "ix_appointment_vehicle_id",
                table: "appointment",
                column: "vehicle_id");

            migrationBuilder.CreateIndex(
                name: "ix_availability_busy_dealership_id_slot",
                schema: "read",
                table: "availability_busy",
                columns: new[] { "dealership_id", "slot" })
                .Annotation("Npgsql:IndexMethod", "gist");

            migrationBuilder.CreateIndex(
                name: "ix_bay_dealership_id",
                table: "bay",
                column: "dealership_id");

            migrationBuilder.CreateIndex(
                name: "ix_customer_email",
                table: "customer",
                column: "email",
                unique: true);

            migrationBuilder.CreateIndex(
                name: "ix_outbox_message_unpublished",
                table: "outbox_message",
                column: "id",
                filter: "published_at IS NULL");

            migrationBuilder.CreateIndex(
                name: "ix_technician_dealership_id",
                table: "technician",
                column: "dealership_id");

            migrationBuilder.CreateIndex(
                name: "ix_vehicle_customer_id",
                table: "vehicle",
                column: "customer_id");

            migrationBuilder.CreateIndex(
                name: "ix_vehicle_vin",
                table: "vehicle",
                column: "vin",
                unique: true);

            // The rule this service exists to enforce (R2), as a guarantee the database keeps no matter what
            // the application does. With one writer per dealership these should never fire; they are the
            // backstop if a stale worker ever writes after losing its partition (D-007).
            migrationBuilder.Sql("""
                ALTER TABLE appointment
                  ADD CONSTRAINT no_technician_overlap EXCLUDE USING gist (technician_id WITH =, slot WITH &&),
                  ADD CONSTRAINT no_bay_overlap        EXCLUDE USING gist (bay_id WITH =, slot WITH &&),
                  ADD CONSTRAINT ck_appointment_slot_bounded CHECK (NOT isempty(slot) AND lower_inc(slot) AND NOT upper_inc(slot)
                                                                     AND NOT lower_inf(slot) AND NOT upper_inf(slot));
                """);
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "appointment");

            migrationBuilder.DropTable(
                name: "availability_busy",
                schema: "read");

            migrationBuilder.DropTable(
                name: "opening_hours");

            migrationBuilder.DropTable(
                name: "outbox_message");

            migrationBuilder.DropTable(
                name: "partition_lease");

            migrationBuilder.DropTable(
                name: "bay");

            migrationBuilder.DropTable(
                name: "booking_request");

            migrationBuilder.DropTable(
                name: "service_type");

            migrationBuilder.DropTable(
                name: "technician");

            migrationBuilder.DropTable(
                name: "vehicle");

            migrationBuilder.DropTable(
                name: "dealership");

            migrationBuilder.DropTable(
                name: "customer");
        }
    }
}
