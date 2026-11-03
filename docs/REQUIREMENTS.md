# Requirements: v0.2

Status: **implemented**, 2026-11-03. This is Keyloop technical assessment, Scenario A: the Unified Service Scheduler.
Source: the Keyloop technical assessment brief (V1.0). IDs are stable. Decisions are in
[DECISIONS.md](DECISIONS.md).

## Goal

Replace manual booking of workshop appointments. A request for a vehicle + service type +
dealership + time is confirmed **only if** a service bay **and** a qualified technician are both
free for the **whole** service duration. The confirmed booking is stored.

## Core requirements (from the brief)

| ID | Requirement | Acceptance criterion (our wording) |
|---|---|---|
| R1 | **Resource-constrained booking**: request an appointment for a vehicle, a service type and a dealership at a desired start time | `POST /appointments` accepts the request, validates it, and works out the end time from the service type's duration |
| R2 | **Real-time availability check**: a ServiceBay **and** a qualified Technician must both be free for the full `[start, end)` before confirming | No two appointments ever overlap on the same bay or the same technician, **including under concurrent requests**. A technician without the skill is never assigned |
| R3 | **Confirmed appointment record**: create a persistent Appointment that links the customer, vehicle, technician and bay | The record survives a restart. It can be read back with all four links. A failed booking leaves nothing behind |

## Stretch (only if R1–R3 are solid)

| ID | Requirement | Why | Status |
|---|---|---|---|
| S1 | **Hold → confirm**: a short-lived hold (TTL) before confirmation | Shows two-phase reservation; matches a real UX where the customer reviews before confirming | Not built (D-004) |
| S2 | `GET /availability`: find free slots for a service type, dealership and day | The read side of the same problem; it's where a separate read model (CQRS) would come in later | **Built**: `GET /availability` over a projected read model |
| S3 | Cancel an appointment (frees the resources) | Smallest change that proves resources are released | Not built |
| S4 | `AppointmentConfirmed` event via a transactional outbox | Integration with other Keyloop systems without dual writes | **Built**: outbox → `appointment-events` |

## Deliverables checklist (from the brief)

- [x] **System Design Document** ([DESIGN.md](DESIGN.md)): architecture diagram, role of each component, data flow, technology choices with reasons, observability strategy, **GenAI-in-design section**
- [x] **Code**: one layer built fully (backend, see D-001), the other mocked (OpenAPI spec + cURL examples)
- [x] `README.md`: build / run / test instructions
- [ ] `README.md`: **AI Collaboration Narrative** section (strategy, verification, quality ownership)
- [x] **Tests that validate the core business logic** (overlap rules, skill matching, concurrent booking)
- [ ] **Video, 5–10 min**: intro + scenario, design + implementation highlights, AI story (1–2 min), demo, lessons and challenges

## How it's graded → where we show it

| Dimension | Where the evidence lives |
|---|---|
| Problem solving & system design | DESIGN.md, DECISIONS.md (options with trade-offs), the concurrency ladder |
| Technical execution | Code, tests, the concurrent-booking test |
| AI engineering & verification | The README narrative, the AI collaboration log and the video |
| Communication | README, DESIGN.md, video |

## Assumptions (the brief is ambiguous on purpose; each one goes in the design doc)

Status: **applied** in the implementation; A5's technician shifts are not modelled.

| ID | Assumption | Why this default |
|---|---|---|
| A1 | Each service type has a **fixed duration** and a **required skill** | The brief needs "duration" and "qualified" to mean something concrete |
| A2 | A technician is **qualified** if their skills include the required skill | Simplest model that makes R2 testable |
| A3 | Bays are **generic** (any bay fits any service) in v1. Bay types (e.g. lift, EV) are an extension point | Keeps the model small; it's cheap to add a `bay_type` later |
| A4 | Technicians and bays belong to **one dealership** | No cross-site sharing in v1 |
| A5 | Bookings must fall inside the dealership's **opening hours** (and technician shifts, if time allows) | Without this, "availability" is only about clashes |
| A6 | Times are stored in **UTC** and each dealership has an IANA **time zone** | Avoids daylight-saving bugs; the API takes ISO-8601 with an offset |
| A7 | Intervals are **half-open** `[start, end)`: a 09:00–10:00 job and a 10:00–11:00 job don't clash | Standard convention; easy to test at the boundary |
| A8 | Customers and vehicles are **looked up or created** by the booking call. No authentication in v1 (stubbed) | Auth is out of scope for the brief; it's noted as a future concern |
| A9 | If several technicians or bays fit, one is chosen by a **pluggable strategy** (default: first-fit, by id) | Deterministic for tests; least-loaded is an easy swap |
| A10 | One appointment = one technician + one bay for the whole duration | No split or multi-technician jobs |
| A11 | "Real-time" = decided by the dealership's single writer and **committed atomically** with the booking, not read from a pre-computed calendar. The client gets the decision in the same call when it arrives within 2 s (D-006) | That is what R2's correctness depends on |
