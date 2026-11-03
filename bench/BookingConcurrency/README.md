# Booking concurrency benchmark

A design-phase experiment, separate from the service code. It compares where the
"no double-booking" rule lives, using the same schema and the same request streams for every approach:

| Approach | How a booking is decided |
|---|---|
| `o1-naive` | Each request runs one SQL statement that picks the first free technician and bay and inserts. The GiST exclusion constraints reject overlaps; losers retry (up to 5 attempts, jittered backoff). |
| `o1-lock` | Same statement, but first takes `pg_advisory_xact_lock(dealership, day)`, so requests for the same dealership-day queue inside Postgres instead of colliding. |
| `o2-single-writer` | Requests are routed to 16 partitions by dealership. One worker per partition decides from an in-memory schedule and writes each drained batch (≤ 64) in one transaction. The constraints remain as a backstop. |

Results and charts: [docs/benchmarks/BOOKING-CONCURRENCY.md](../../docs/benchmarks/BOOKING-CONCURRENCY.md).

## Run it

Needs Docker, the .NET 10 SDK and Python 3 (standard library only).

```bash
./run.sh smoke   # ~1 minute, one point per scenario
./run.sh full    # ~35 minutes, writes results/full.csv and the SVGs in docs/benchmarks
```

## What is measured

- **Load is open-loop.** Arrivals follow a Poisson process at the offered rate whether or not earlier
  requests finished, and latency is measured from each request's *scheduled* arrival. A closed loop
  would slow down with the system and hide the queueing (coordinated omission).
- **Latency** = time until the client knows the outcome: booked, "no capacity" or error. For O2 this is
  until the batch holding the booking has committed, not the `202 Accepted`.
- **Goodput** = correct answers (booked or "no capacity") per second that arrived within 500 ms.
- **DB transactions per booking** counts every attempt, retry, batch and schedule load.
- **Peak connections** = the most database operations in flight at once (pool limit 50).
- After every run the harness checks that no technician or bay has overlapping bookings and that the
  number of rows written equals the number of confirmations.

## Scenarios

| Scenario | Shape |
|---|---|
| `normal` | 500 dealerships × 8 technicians × 6 bays, requests spread over 30 days. Real conflicts are rare. |
| `hot` | 1 service centre × 40 technicians × 30 bays over 60 days: a recall campaign. |
| `burst` | N requests (10–200) for the same dealership, day and hour at the same instant; 6 can fit. |

Jobs are 1 h (70%) or 2 h (30%) inside 08:00–18:00. Seven past days are pre-filled to half capacity
so the planner sees a realistic table.

## Known limits

- An in-process channel stands in for the broker, plus a fixed 5 ms delay per command for the
  network hop. A real broker adds its own latency variance, durability cost and partition rebalancing.
- O2's in-memory schedules are loaded before each run starts (a long-running worker already has them).
  The cold-start cost after a deploy or rebalance is not measured.
- Client, API logic and Postgres share one laptop (Postgres capped at 4 CPUs, 2 GB). Absolute numbers
  depend on the machine; compare the approaches with each other, not with production.
- Postgres `deadlock_timeout` is the default 1 s. The O1-naive burst numbers move with it.
