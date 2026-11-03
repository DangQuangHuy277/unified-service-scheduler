#!/usr/bin/env bash
# N customers book the same slot at the same instant. Expect exactly "capacity" confirmations,
# a clean 409 for everyone else, and zero overlapping bookings in the database.
#
#   demo/burst.sh [requests=50] [dealership=3]
set -euo pipefail
source "$(dirname "$0")/_lib.sh"

N="${1:-50}"
DEALER="${2:-3}"
require_stack

ZONE=$(sql "SELECT time_zone FROM dealership WHERE id = $DEALER")
[[ -n "$ZONE" ]] || { echo "No dealership $DEALER in the demo data (try 1, 2 or 3)."; exit 1; }
DAY=$(random_weekday)
START=$(local_start "$DAY" "10:00" "$ZONE")
TECHS=$(sql "SELECT count(*) FROM technician t JOIN service_type s ON s.code = 'OIL'
             WHERE t.dealership_id = $DEALER AND s.required_skill = ANY(t.skills)")
BAYS=$(sql "SELECT count(*) FROM bay WHERE dealership_id = $DEALER")
CAPACITY=$(( TECHS < BAYS ? TECHS : BAYS ))
RUN=$(printf '%08d' $((RANDOM * RANDOM % 100000000)))   # makes customers and VINs unique per run

echo "Dealership $DEALER ($ZONE): $TECHS technicians qualified for an oil change, $BAYS bays → capacity $CAPACITY"
echo "Firing $N simultaneous POST /appointments for $START ..."

RESULTS=$(mktemp)
BODIES=$(mktemp)
trap 'rm -f "$RESULTS" "$BODIES"' EXIT
for i in $(seq 1 "$N"); do
  printf '{"dealershipId":%s,"serviceType":"OIL","start":"%s","customer":{"name":"Burst %s","email":"burst-%s-%s@example.com"},"vehicle":{"vin":"%s","make":"Honda","model":"Accord"}}\n' \
    "$DEALER" "$START" "$i" "$RUN" "$i" "$(vin "$i" "$RUN")"
done > "$BODIES"
tr '\n' '\0' < "$BODIES" | xargs -0 -P "$N" -I{} curl -s -o /dev/null -w '%{http_code} %{time_total}\n' \
  -X POST "$API/appointments" -H 'Content-Type: application/json' -d '{}' > "$RESULTS"

CREATED=$(grep -c '^201 ' "$RESULTS" || true)
CONFLICT=$(grep -c '^409 ' "$RESULTS" || true)
OTHER=$(( N - CREATED - CONFLICT ))
SLOWEST=$(cut -d' ' -f2 "$RESULTS" | sort -n | tail -1)
BOOKED=$(sql "SELECT count(*) FROM appointment WHERE dealership_id = $DEALER AND lower(slot) = '$START'")
OVERLAPS=$(overlaps)

echo
echo "  $CREATED × 201 Created   $CONFLICT × 409 No capacity   $OTHER × other    slowest answer ${SLOWEST}s"
echo "  appointments stored for that slot: $BOOKED    overlapping bookings in the database: $OVERLAPS"
if [[ "$CREATED" == "$CAPACITY" && "$BOOKED" == "$CAPACITY" && "$OTHER" == 0 && "$OVERLAPS" == 0 ]]; then
  echo "  $(green "✔ exactly capacity booked, everyone else told no, nothing double-booked")"
else
  echo "  $(red "✘ unexpected result, see above")"; exit 1
fi
