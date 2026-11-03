# Shared helpers for the demo scripts. Sourced, not run.
# Needs: bash, curl, python3, docker compose, and the stack from deploy/docker-compose.yml running.

DEMO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="$DEMO_DIR/../deploy/docker-compose.yml"
API="${API:-http://localhost:8080}"

compose() { docker compose -f "$COMPOSE_FILE" "$@"; }

# Run SQL in the stack's Postgres and print the bare result.
sql() { compose exec -T postgres psql -U scheduler -d scheduler -At -c "$1"; }

overlaps() {
  sql "SELECT (SELECT count(*) FROM appointment a JOIN appointment b
                 ON a.id < b.id AND a.technician_id = b.technician_id AND a.slot && b.slot)
            + (SELECT count(*) FROM appointment a JOIN appointment b
                 ON a.id < b.id AND a.bay_id = b.bay_id AND a.slot && b.slot)"
}

require_stack() {
  if ! curl -sf "$API/health" > /dev/null; then
    echo "The API isn't answering at $API. Start the stack first:"
    echo "  docker compose -f deploy/docker-compose.yml up --build -d"
    exit 1
  fi
}

# A random future date (2027–2028) that falls on a weekday, as YYYY-MM-DD.
random_weekday() {
  python3 -I -c "
import datetime, random
while True:
    d = datetime.date(2027, 1, 1) + datetime.timedelta(days=random.randrange(700))
    if d.weekday() < 5: print(d); break"
}

# ISO 8601 start time with the right UTC offset for a local wall-clock time in an IANA zone.
local_start() {  # $1 = YYYY-MM-DD, $2 = HH:MM, $3 = IANA zone
  python3 -I -c "
import datetime, sys
from zoneinfo import ZoneInfo
d, t, z = sys.argv[1:4]
print(datetime.datetime.fromisoformat(f'{d}T{t}').replace(tzinfo=ZoneInfo(z)).isoformat())" "$1" "$2" "$3"
}

# A valid 17-character VIN (no I, O or Q) that is unique per run and request number.
vin() { printf '%s%09d' "$2" "$1"; }  # $1 = number, $2 = 8-character prefix

green() { printf '\033[32m%s\033[0m' "$1"; }
red()   { printf '\033[31m%s\033[0m' "$1"; }
