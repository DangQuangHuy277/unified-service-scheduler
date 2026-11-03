# AI collaboration log

How an AI coding agent (Claude Code) was used to design, build and verify this service, and what checking
its work caught. Each entry links to the evidence in this repository.

## How the work was split

| The AI | Me |
|---|---|
| Wrote option comparisons as weighted tables (stack, architecture, broker, API style) | Set the weights against the brief's grading criteria and made each decision ([DECISIONS.md](DECISIONS.md)) |
| Explained trade-offs and failure modes | Treated each explanation as a hypothesis and tested the ones the design depended on |
| Wrote the benchmark harness, the code and the tests from an approved plan | Approved the plan, then required each layer to build with warnings as errors and pass its tests before the next started |
| Ran the system and reported results | Read the actual output (traces, query results, timings), not just the pass/fail summary |

Three rules carried the verification:
1. **Test claims on the real thing.** Concurrency, fencing and replay only exist against a real Postgres and
   a real Kafka, so integration tests use Testcontainers, never in-memory fakes.
2. **A green run is not proof.** A flaky test, a suspiciously fast benchmark or a demo that "passed" too
   quickly gets a cause before it gets a retry.
3. **Run the whole system.** Every scenario was replayed on the Docker Compose stack, not only in tests.

## Log

| # | Phase | What the AI proposed or produced | How I checked it | Outcome |
|---|---|---|---|---|
| 1 | Stack | A weighted comparison of .NET, Java, Go, TypeScript and Python | Checked its claims against Keyloop's public job postings and Npgsql's range-type support | C# / .NET 10 ([D-003](DECISIONS.md)) |
| 2 | Architecture | An explanation that with database-enforced booking (O1), a losing concurrent insert "gets `23P01`" (exclusion violation) | Raced 2 and 20 simultaneous inserts for one technician and slot against real Postgres 16, 5 trials | **Incomplete.** Simultaneous inserts deadlock (`40P01`) and Postgres breaks one deadlock per second: the 20th request waited ~19 s. Correctness held; availability didn't. This is what made a measured comparison necessary |
| 3 | Benchmark | A harness comparing O1, O1 + a per-day lock, and one writer per dealership (O2), with charts | Traced every anomaly to a query plan or system metric before trusting a number | **Biased three ways, all fixed before publishing:** background checkpoint/vacuum I/O ran during O1's measurement (p50 566 ms → 4.9 ms once removed); O2 started with cold schedules a long-running worker wouldn't have (48 → 9.6 ms); Docker's port proxy burned 40% CPU. Final run: 153 runs, median of 3 trials ([results](benchmarks/BOOKING-CONCURRENCY.md)) |
| 4 | Architecture | O2 over O1, from the benchmark | Weighed it against realistic dealer volume, which O1 + lock would also handle | O2 chosen for isolation and headroom, with that caveat written into [D-004](DECISIONS.md) |
| 5 | Messaging | Kafka, a Postgres queue table, or RabbitMQ to carry commands | Checked each against the benchmark's own finding that O1's ceiling was one database commit per request | Kafka: a Postgres queue would bring back exactly that per-request commit ([D-005](DECISIONS.md)) |
| 6 | Build | Domain, schema, worker, API, read side and tests (~3,200 lines) | Layer by layer, each gated on a warnings-as-errors build and its tests | All 11 integration tests passed on the first full run against real Postgres and Kafka |
| 7 | Review | Keep minimal APIs (scored 85% vs 83% for controllers) | My call: controllers, for reviewer familiarity and declarative validation | Converted with the HTTP tests unchanged as the safety net. **A new test caught a defect:** validation errors were keyed `Vehicle.Vin` instead of the `vehicle.vin` clients send (positional record parameters keep their C# names). Fixed with init-only properties ([D-009](DECISIONS.md)) |
| 8 | System run | Dockerfile, compose file and demo seed data | Started the stack instead of assuming it worked | **Two bugs in seed SQL that no test executed:** EF Core's raw-SQL API reads `{a,b}` array literals as format placeholders, and `UNION ALL` typed opening times as text. Fixed, and the seed is now covered by `Demo_seed_is_repeatable_and_loads_as_reference_data` |
| 9 | Proof | The claim that the system meets R2 under concurrency and survives a worker failure | On the running stack: 60 simultaneous requests for one slot, then 400 bookings with one of two workers stopped midway, then an overlap query | 3 × 201 / 57 × 409 (3 qualified technicians were the limit, not the 4 bays); 400/400 answered through the failover; 0 overlapping bookings |
| 10 | Flaky test | A test failed once and passed on rerun | Matched it to a measurement instead of rerunning until green: the first booking in compose took 2.45 s, past the API's 2 s reply window, so it correctly became a 202 | Startup warm-up of EF Core and the Kafka producer (first booking 2.45 s → 0.26 s); tests that assert a final outcome now ask for one with `Prefer: wait=10` |
| 11 | Observability | Tracing set up and reported working | Opened the Aspire dashboard and looked beyond the trace I expected | One trace runs API → Kafka → worker as intended, but ~1,400 single-span traces from idle polling buried it. Database spans without a parent operation are now filtered |
| 12 | Demo | Scripts for the burst and failover demonstrations | Ran each live and read the numbers, not just the final ✔ | **The failover script reported success without a failover:** on a warm stack all requests finished before the worker was stopped. It now stops a worker after a quarter of the answers and fails unless one worker ends up owning every partition |

## What the AI got wrong, and what caught it

| Mistake | Caught by |
|---|---|
| Conflict path described as "the loser gets `23P01`"; in reality simultaneous losers deadlock | A real-Postgres race test (#2) |
| A benchmark that measured background I/O, cold caches and a proxy instead of the approaches | Questioning numbers that didn't fit (#3) |
| Seed SQL that had never been executed | Running the full stack (#8) |
| Validation errors keyed by C# names | A test written for the conversion (#7) |
| A 2 s reply window that didn't account for cold start | Not accepting a rerun as a fix (#10) |
| Telemetry tracing idle loops | Reading the dashboard, not just finding one good trace (#11) |
| A demo that declared success without doing the thing it demonstrates | Reading the output during rehearsal (#12) |

## Open follow-ups

- **The same vehicle can be booked into two overlapping slots** (found during the demo rehearsal). R2 is
  about technicians and bays, so this is outside the brief, but a real workshop would reject it. The fix is a
  per-vehicle check in the planner plus a third exclusion constraint on `(vehicle_id, slot)`.
- Hold → confirm (S1) and cancellation (S3) are not built.
