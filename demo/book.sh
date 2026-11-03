#!/usr/bin/env bash
# One booking, printed as a single readable line.
#
#   demo/book.sh <dealership> <service> <start ISO-8601 with offset> <customer name> [extra curl args...]
#   demo/book.sh 1 OIL 2027-06-15T10:00:00+01:00 "Ben Carter"
#   demo/book.sh 1 OIL 2027-06-15T10:00:00+01:00 "Ben Carter" -H 'Idempotency-Key: ben-1'
#   demo/book.sh 1 OIL 2027-06-15T10:00:00+01:00 "Ben Carter" -H 'Prefer: respond-async'
set -euo pipefail
source "$(dirname "$0")/_lib.sh"

DEALER="$1" SERVICE="$2" START="$3" NAME="$4"; shift 4
SLUG=$(echo "$NAME" | tr 'A-Z ' 'a-z.')
VIN=$(printf '%s' "$NAME" | sha512sum | tr -dc '0-9' | head -c 17)   # stable, valid VIN per customer name

BODY=$(printf '{"dealershipId":%s,"serviceType":"%s","start":"%s","customer":{"name":"%s","email":"%s@example.com"},"vehicle":{"vin":"%s","make":"Volkswagen","model":"Golf"}}' \
  "$DEALER" "$SERVICE" "$START" "$NAME" "$SLUG" "$VIN")
RESPONSE=$(curl -s -w '\n%{http_code} %{time_total}' -X POST "$API/appointments" -H 'Content-Type: application/json' "$@" -d "$BODY")

python3 -I - "$RESPONSE" <<'PY'
import json, sys
body, status_line = sys.argv[1].rsplit("\n", 1)
status, seconds = status_line.split()
ms = f"{float(seconds) * 1000:.0f} ms"
d = json.loads(body) if body.strip() else {}
if status == "201":
    print(f"201 Created  ({ms})  appointment {d['id']}: {d['service']['name']} {d['start'][11:16]}–{d['end'][11:16]}"
          f"  technician {d['technician']['name']} ({d['technician']['id']}), {d['bay']['name']}  for {d['customer']['name']}")
elif status == "202":
    print(f"202 Accepted ({ms})  still deciding; poll /booking-requests/{d['requestId']}")
elif status == "400":
    print(f"400 Bad Request ({ms})  " + "; ".join(f"{k}: {' '.join(v)}" for k, v in d.get("errors", {}).items()))
else:
    print(f"{status} ({ms})  {d.get('outcome', '')}: {d.get('title', body)}")
PY
