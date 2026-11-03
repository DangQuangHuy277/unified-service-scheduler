#!/usr/bin/env bash
# Reproduce the booking-concurrency benchmark: throwaway Postgres → suite → charts.
# Usage: ./run.sh [smoke|full]     (full takes ~35 minutes)
set -euo pipefail
cd "$(dirname "$0")"

SUITE="${1:-full}"
NAME=usx-bench-pg
PG_IMAGE="${PG_IMAGE:-postgres:16}"

docker rm -f "$NAME" >/dev/null 2>&1 || true
docker run --rm -d --name "$NAME" --cpus=4 --memory=2g -e POSTGRES_PASSWORD=bench "$PG_IMAGE" \
  -c max_connections=200 -c shared_buffers=256MB >/dev/null
trap 'docker stop "$NAME" >/dev/null' EXIT

# Connect to the container's own IP: the published-port proxy adds CPU load and latency noise.
IP=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$NAME")
until docker exec "$NAME" pg_isready -U postgres >/dev/null 2>&1; do sleep 1; done
sleep 2

dotnet build -c Release >/dev/null
dotnet bin/Release/net10.0/BookingConcurrency.dll --suite "$SUITE" --out "results/$SUITE.csv" \
  --conn "Host=$IP;Port=5432;Username=postgres;Password=bench;Database=postgres;Maximum Pool Size=50;Timeout=30;Command Timeout=60;No Reset On Close=true"

python3 plot.py "results/$SUITE.csv" ../../docs/benchmarks
