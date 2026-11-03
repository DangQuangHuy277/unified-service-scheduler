#!/usr/bin/env bash
# Steady booking traffic while one of the two workers is stopped mid-run. Its Kafka partitions move to
# the other worker, which takes new fencing epochs; every request still gets a final answer.
#
#   demo/failover.sh [requests=600]
set -euo pipefail
source "$(dirname "$0")/_lib.sh"

N="${1:-600}"
require_stack

# Start from two running workers, each owning some partitions.
compose up -d --no-recreate worker > /dev/null 2>&1
compose start worker > /dev/null 2>&1 || true
echo -n "Waiting for two workers to share the 16 partitions"
for _ in $(seq 1 60); do
  [[ "$(sql "SELECT count(DISTINCT owner) FROM partition_lease")" == 2 ]] && break
  echo -n "."; sleep 1
done
echo
sql "SELECT '  ' || owner || ' owns ' || count(*) || ' partitions' FROM partition_lease GROUP BY owner ORDER BY owner"

RUN=$(printf '%08d' $((RANDOM * RANDOM % 100000000)))
BODIES=$(mktemp)
RESULTS=$(mktemp)
trap 'rm -f "$BODIES" "$RESULTS"' EXIT
# Spread over London and Ho Chi Minh City, many days and hours, so most requests can be confirmed.
python3 -I - "$N" "$RUN" > "$BODIES" <<'PY'
import datetime, json, random, sys
from zoneinfo import ZoneInfo
n, run = int(sys.argv[1]), sys.argv[2]
for i in range(1, n + 1):
    dealer, zone = (1, "Europe/London") if i % 2 else (2, "Asia/Ho_Chi_Minh")
    while (day := datetime.date(2027, 1, 1) + datetime.timedelta(days=random.randrange(700))).weekday() >= 5:
        pass
    start = datetime.datetime.combine(day, datetime.time(8 + i % 8), ZoneInfo(zone)).isoformat()
    print(json.dumps({"dealershipId": dealer, "serviceType": "OIL", "start": start,
                      "customer": {"name": f"Load {i}", "email": f"load-{run}-{i}@example.com"},
                      "vehicle": {"vin": f"{run}{i:09d}", "make": "Toyota", "model": "Corolla"}}))
PY

echo "Sending $N bookings, 6 at a time ..."
( tr '\n' '\0' < "$BODIES" | xargs -0 -P 6 -I{} curl -s -o /dev/null -w '%{http_code} %{time_total} %header{location}\n' \
    -X POST "$API/appointments" -H 'Content-Type: application/json' -d '{}' > "$RESULTS" ) &
LOAD=$!

# Stop a worker once a quarter of the requests have been answered, so the handover happens mid-traffic
# (a fixed delay is not enough: on a warm stack 300 bookings can finish in under 2 s).
until (( $(wc -l < "$RESULTS") >= N / 4 )) || ! kill -0 "$LOAD" 2> /dev/null; do sleep 0.05; done
VICTIM=$(compose ps -q worker | head -1)
echo "  ... $(wc -l < "$RESULTS") of $N answered; stopping worker container ${VICTIM:0:12}"
docker stop -t 10 "$VICTIM" > /dev/null
wait "$LOAD"

# A request still deciding when the wait ran out answered 202; follow each one to its outcome.
PENDING=$(grep -c '^202 ' "$RESULTS" || true)
for location in $(awk '$1 == 202 { print $3 }' "$RESULTS" | tr -d '\r'); do
  for _ in $(seq 1 50); do
    curl -s "$API$location" | grep -q '"status":"pending"' || break
    sleep 0.2
  done
done

CREATED=$(grep -c '^201 ' "$RESULTS" || true)
CONFLICT=$(grep -c '^409 ' "$RESULTS" || true)
ERRORS=$(awk '$1 >= 500 || $1 == 000' "$RESULTS" | wc -l)
read -r P50 P99 MAX < <(cut -d' ' -f2 "$RESULTS" | sort -n | awk '{a[NR]=$1} END {print a[int(NR*0.5)], a[int(NR*0.99)], a[NR]}')
OVERLAPS=$(overlaps)

echo
echo "  $CREATED × 201   $CONFLICT × 409 (slot already full)   $PENDING × 202 (followed to an outcome)   $ERRORS × errors"
echo "  latency p50 ${P50}s   p99 ${P99}s   max ${MAX}s (the handover pause shows up in the tail)"
sql "SELECT '  ' || owner || ' now owns ' || count(*) || ' partitions, epochs up to ' || max(epoch) FROM partition_lease GROUP BY owner"
echo "  overlapping bookings in the database: $OVERLAPS"
OWNERS=$(sql "SELECT count(DISTINCT owner) FROM partition_lease")
if [[ "$OWNERS" != 1 ]]; then
  echo "  $(red "✘ the load finished before the handover; rerun with more requests, e.g. demo/failover.sh 1500")"
elif [[ "$ERRORS" == 0 && "$OVERLAPS" == 0 ]]; then
  echo "  $(green "✔ every request answered through the failover, nothing double-booked")"
else
  echo "  $(red "✘ unexpected result, see above")"
fi

echo "Restarting the stopped worker; partitions will rebalance back to two owners."
docker start "$VICTIM" > /dev/null
