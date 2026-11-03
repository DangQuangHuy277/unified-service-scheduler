# Decisions

Append-only. To change a decision, add a new entry that supersedes the old one.

## D-001: Build the backend; mock the frontend
2026-11-02. **Proposed.** All of the depth is in the backend: the atomic check, the constraints and
the concurrency. The client is stubbed with an OpenAPI spec, cURL examples and an `.http` file.
A frontend would mostly be a date picker.

## D-002: Conflict-control strategy, built as Level 2 + Level 4; Level 5 on paper
2026-11-02. **Proposed.** Benchmarked against each other in DESIGN.md.

| Level | Strategy | Role here |
|---|---|---|
| 0 | Check availability, then insert | **Anti-pattern.** Kept as a failing test to show the race |
| 1 | Pessimistic `SELECT … FOR UPDATE` on the technician and bay rows | Compared in the doc |
| 2 | **The database enforces it**: Postgres `EXCLUDE USING gist` on `(technician_id =, slot &&)` and `(bay_id =, slot &&)` | **Built.** The final guarantee no matter how the app code is written |
| 3 | Optimistic: a version number per resource-day, retry on conflict | Compared in the doc |
| 4 | **Hold → confirm** with a TTL | **Built if time allows** (S1). Holds also occupy the exclusion constraint |
| 5 | One writer per dealership (queue partitioned by `dealership_id`) + CQRS availability read model + outbox | **Design doc only.** The scaling path |

## D-003: Language and framework: C# on .NET 10 (LTS) with ASP.NET Core
2026-11-02. **Decided.**

**Context.** D-002 depends on three things from the stack: mapping Postgres `tstzrange`, catching
the exclusion-violation error (`23P01`) as a typed conflict, and running concurrency tests against
a **real** Postgres. In-memory or SQLite test doubles don't enforce exclusion constraints, so they
would hide the very race we're trying to prove is fixed. Gate: the stack must support Postgres
exclusion constraints, Testcontainers and OpenTelemetry. All five candidates pass.

**Options**
- **N, C# / ASP.NET Core 10.** EF Core + Npgsql (native `NpgsqlRange<DateTime>` ↔ `tstzrange`),
  xUnit + Testcontainers, built-in OpenAPI and metrics.
- **J, Java 25 / Spring Boot.** JPA or jOOQ, Testcontainers, Micrometer. Hibernate needs an extra
  library to map range types.
- **G, Go.** pgx + sqlc, testcontainers-go. Very explicit code; observability is wired by hand.
- **T, TypeScript / Node.** Fastify or NestJS, Kysely or `pg`. Fast to write; range types come back
  as strings.
- **P, Python / FastAPI.** SQLAlchemy + psycopg 3. Fast to write; dynamically typed.

| Criterion (why it matters) | Weight | N .NET | J Java | G Go | T TypeScript | P Python |
|---|---|---|---|---|---|---|
| Alignment with Keyloop's stack (public postings are mostly C#/.NET + AWS) | ×2 | 5/5: direct match | 2/5: not in postings | 2/5: not in postings | 4/5: some TS + AWS roles | 1/5: not in postings |
| Range type + conflict-error ergonomics (heart of D-002) | ×2 | 5/5: `NpgsqlRange`, `PostgresException.SqlState` | 3/5: extra mapping library or jOOQ | 5/5: `pgtype.Range`, `PgError.Code` | 2/5: ranges parsed from strings | 4/5: psycopg `Range` |
| Concurrency tests against a real DB | ×2 | 4/5: Testcontainers + `Task.WhenAll` | 5/5: Testcontainers started in Java | 5/5: goroutines + `-race` | 4/5: parallel connections from one event loop | 4/5: pytest + threads/async |
| Built-in observability | ×1 | 5/5: OTel + ASP.NET Core metrics | 4/5: Micrometer + OTel agent | 3/5: manual otel-go wiring | 3/5: auto-instrumentation, ESM friction | 3/5: works, less built in |
| Easy to verify generated code (less hidden behaviour) | ×1 | 4/5: EF has some magic; the booking path uses explicit SQL | 2/5: proxy/annotation magic | 5/5: fully explicit | 3/5: loose at runtime | 3/5: dynamic typing |
| Delivery speed | ×1 | 4/5: minimal setup, built-in OpenAPI | 3/5: heavier setup | 3/5: more hand wiring | 5/5: fastest | 5/5: fastest |
| **Weighted total** (max 45) | | **41 (91%)** | 29 (64%) | 35 (78%) | 31 (69%) | 29 (64%) |

**Decision.** N. Go is the runner-up: it's even more explicit and has a nicer race-detector story,
but it's weaker on alignment with Keyloop and on built-in observability.
**Consequences.** EF Core owns the schema and migrations. The exclusion constraints and the
`btree_gist` extension are raw SQL inside a migration, because EF can't express them. The booking
write path maps `23P01` to a domain `SlotConflict` and never relies on a check-then-insert.

## D-004: Build O2, one writer per dealership with a CQRS read side
2026-11-02. **Decided.** Supersedes D-002's "Level 5 on paper"; confirms D-001 (backend). The exclusion
constraints from D-002 Level 2 stay, as a backstop. Hold → confirm (Level 4, S1) is not built.

**Context.** The benchmark ([benchmarks/BOOKING-CONCURRENCY.md](benchmarks/BOOKING-CONCURRENCY.md),
153 runs) measured where the "no double-booking" rule should live:

| | O1-naive (constraint + retry) | O1 + per-dealership-day lock | O2 single writer |
|---|---|---|---|
| Correct (no overlaps) | yes | yes | yes |
| 10–200 requests for one slot at once | never finished: inserts deadlock, Postgres breaks one per second | everyone answered, slowest 34 → 300 ms (grows with N) | everyone answered in ≈ 10 ms |
| Plateau (normal day) | ≈ 2,500 answers/s | ≈ 2,500 answers/s | not reached at 8,000 req/s |
| Database connections | all 50 from 1,000 req/s | all 50 from 1,000 req/s | ≤ 16 (one per partition) |
| Median latency below saturation | 5–6 ms | 5–6 ms | 9–16 ms (includes a simulated 5 ms broker hop) |

**Decision.** O2. One busy dealership can't take every connection from the others, and capacity grows
with partitions instead of being capped by one database's commit rate. The price is a few milliseconds per
booking and a broker to run.

**Honest scope note.** A dealer network generates tens of bookings per second, far below O1 + lock's
plateau. O2 is chosen for isolation, headroom and as the more instructive design; O1 + lock would be
the pragmatic choice for a single-region launch.

## D-005: Apache Kafka carries booking commands; the API never writes to the database per request
2026-11-02. **Decided.** Kafka 3.9 in KRaft mode, the official `apache/kafka` image (first proposed as
Redpanda; switched to Apache Kafka for the reference broker). Commands are keyed by `dealership_id`, so
one dealership always lands on one partition and therefore one worker.

**Options**
- **K1, Kafka.** Partitioned log; a consumer group gives each partition one owner and moves it on failure.
- **K2, a Postgres table as the queue.** No extra infrastructure, advisory locks for ownership.
- **K3, RabbitMQ.** One queue per partition with single-active-consumer.

| Criterion (why it matters) | Weight | K1 Kafka | K2 Postgres queue | K3 RabbitMQ |
|---|---|---|---|---|
| Keeps O2's measured benefit (no DB commit per request) | ×3 | 5/5: batched log appends | 1/5: one commit per request, O1's bottleneck again | 4/5: per-message acks |
| Exclusive ownership + safe failover | ×3 | 4/5: consumer group + our fencing epoch (D-007) | 5/5: advisory lock held while the session lives | 3/5: hand-kept partition map |
| Delivery risk | ×2 | 3/5: rebalances and offsets to get right | 4/5: only SQL | 3/5: exchange topology |
| Local and test footprint | ×1 | 3/5: one more container | 5/5: Postgres only | 3/5: one more container |
| Fit with Keyloop (AWS MSK, event-driven roles) | ×1 | 5/5 | 2/5 | 3/5 |
| **Weighted total** (max 50) | | **41 (82%)** | 33 (66%) | 33 (66%) |

**Consequence.** The API validates against cached reference data and produces the command; its only
database access on the request path is reads.

## D-006: POST waits up to 2 s for the decision, then answers 202
2026-11-02. **Decided.**
- The worker calls `pg_notify('booking_decided', ids)` inside its batch transaction, so the notification
  is delivered only if the batch commits. Each API instance keeps one LISTEN connection and wakes the
  waiting request. It registers before producing, so a fast decision can't be missed, and reads the
  database once more before giving up, so a missed notification only costs the wait.
- 201 Created (confirmed), 409 (no capacity: try another time), 422 (can never be booked as asked),
  202 Accepted + `Location: /booking-requests/{id}` when no decision arrived in time.
- `Prefer: wait=N` (max 10 s) and `Prefer: respond-async` (RFC 7240) let clients choose.
- Request ids are UUIDv7. With an `Idempotency-Key` header the id is a UUIDv5 of the key, so a retry is
  the same request and gets the same answer. Because UUIDv7 carries its creation time,
  `GET /booking-requests/{id}` can answer 404 for an id never seen after 5 minutes instead of "pending" forever.

## D-007: Fencing epochs guard partition handovers; the exclusion constraints are the backstop
2026-11-02. **Decided.** Kafka can briefly let two consumers believe they own a partition (a paused
process that misses its session timeout, then wakes). On assignment a worker increments
`partition_lease.epoch` and keeps the new value. Every batch transaction starts with
`SELECT epoch … FOR UPDATE` and aborts if the epoch moved. The row lock also makes a takeover wait for the
previous owner's in-flight batch. If anything still slipped through, the GiST exclusion constraints reject
the overlap (`23P01`), the worker counts it (`scheduler.booking.backstop_violations`), reloads the
schedule from the database and retries the batch.

## D-008: At-least-once delivery, exactly-once effect
2026-11-02. **Decided.**
- Kafka offsets are committed only after the batch transaction commits. A crash in between replays the
  batch; the worker skips request ids that already have a `booking_request` row.
- Events leave through a transactional outbox (written in the batch transaction). The relay uses
  `FOR UPDATE SKIP LOCKED`, so several workers can relay; a crash between publishing and marking can
  resend, so the projector is idempotent by appointment id.
- The availability projection doesn't depend on event order, so relays may interleave freely.

## D-009: Controllers ([ApiController]) for the HTTP layer
2026-11-02. **Decided** (first built with minimal APIs, converted after review).

| Criterion (why it matters) | Weight | Minimal APIs | Controllers |
|---|---|---|---|
| Familiar to the reviewer (Keyloop postings name ASP.NET Web API/MVC) | ×2 | 4/5 | 5/5 |
| Fits these handlers (4 endpoints, an orchestrating POST) | ×3 | 4/5: one 9-parameter handler | 4/5: constructor injection, more ceremony |
| Validation and error shape | ×2 | 4/5: hand-written, or `AddValidation()` | 5/5: automatic 400 `ValidationProblem` |
| Cost of changing (code written, tests green) | ×2 | 5/5 | 3/5 |
| Testability | ×1 | 4/5 | 4/5 |
| Overhead / AOT (not the bottleneck) | ×1 | 5/5 | 3/5 |
| Room for many endpoints | ×1 | 4/5 | 5/5 |
| **Weighted total** (max 60) | | 51 (85%) | 50 (83%) |

**Decision.** Controllers, for reviewer familiarity and declarative validation; the scores were a
near-tie. Request models use init-only properties rather than positional records, because MVC validates
positional record parameters under their C# names (`Vehicle.Vin`) instead of the JSON names clients send
(`vehicle.vin`). A test pins this.

## D-010: Swagger UI for the interactive API docs
2026-11-03. **Decided** (replaces Scalar, used briefly). The OpenAPI document still comes from the built-in
`Microsoft.AspNetCore.OpenApi` generator. Only Swashbuckle's standalone UI package
(`Swashbuckle.AspNetCore.SwaggerUI`) renders it, at `/swagger`. Chosen for presenter and reviewer familiarity;
the document, its descriptions and the ready-to-send example for `POST /appointments` are unchanged.
`/scalar` redirects to `/swagger`.

## D-011: All queries behind a data-access layer in `Infrastructure/Data`; EF Core keeps the schema
2026-11-04. **Decided.** SQL had spread over 7 classes in three projects, some of it inline in orchestration
code (the batch processor, the outbox relay, the projector).

**Options**
- **A, keep as is.**
- **B, all SQL, EF Core removed** (migrations as SQL scripts, hand-written reference-data mapping).
- **C, hybrid.** EF Core keeps schema and migrations; every query moves behind purpose-named stores.

| Criterion (why it matters) | Weight | A Keep | B All SQL | C Hybrid |
|---|---|---|---|---|
| Readability for the reviewer (all SQL in one place) | ×3 | 3/5 | 4/5 | 5/5 |
| Correctness risk of the change (one transaction per batch must stay intact) | ×3 | 5/5 | 2/5 | 4/5 |
| Testability | ×2 | 3/5 | 4/5 | 4/5 |
| Migration tooling | ×2 | 5/5 | 3/5 | 5/5 |
| Effort | ×2 | 5/5 | 2/5 | 4/5 |
| Keyloop familiarity (postings list EF) | ×1 | 4/5 | 3/5 | 5/5 |
| **Weighted total** (max 65) | | 54 (83%) | 39 (60%) | **58 (89%)** |

**Placement.** `Scheduler.Infrastructure/Data`, foldered by consumer (`Shared/`, `Write/` for the Worker,
`Read/` for the Api), over a separate project (76%), interfaces in an application layer (73%) and
vertical feature folders (84%; ahead only if cohesion weighs ×4 or more).

**Rules.** No generic `IRepository<T>`: it would force per-row writes and lose the `unnest` batching.
Stores take the caller's `NpgsqlConnection`/`NpgsqlTransaction`, so the batch processor still runs
fence → dedupe → decide → write → notify in one transaction and owns the order. No interfaces: the only
second implementation would be a fake, and the tests deliberately run against real Postgres. Stores return
data-layer records (`AppointmentDetails`, `DecidedBooking`); the Api maps them to its HTTP contract.
Out of the layer on purpose: `PgNotifications` (LISTEN on a dedicated connection) and `DemoData` (the seed).

