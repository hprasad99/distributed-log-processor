#!/bin/bash

TOTAL=10000
BATCH=10
RPS=100   # requests per second
URL="http://localhost:8080/api/logs"
SLEEP=$(echo "scale=4; $BATCH / $RPS" | bc)
LEVELS=("INFO" "WARN" "ERROR" "DEBUG")
SOURCES=("auth-service" "db-service" "payment-service" "user-service" "order-service")

echo "Running load test against the log processing system..."
echo "Waiting for services to be ready..."

until curl -sf -X POST "$URL" \
  -H "content-type: application/json" \
  -d '{"organizationId":"health-check","level":"INFO","source":"load-test","message":"health"}' \
  > /dev/null 2>&1; do
  echo "Waiting for API gateway..."
  sleep 2
done

echo "Services are ready! Sending $TOTAL log events at $RPS req/s (batches of $BATCH)..."

for i in $(seq 1 $TOTAL); do
  ORG="org-$((i % 5))"
  LEVEL="${LEVELS[$((i % ${#LEVELS[@]}))]}"
  SOURCE="${SOURCES[$((i % ${#SOURCES[@]}))]}"

  curl -sf -X POST "$URL" \
    -H "content-type: application/json" \
    -d "{\"organizationId\":\"$ORG\",\"level\":\"$LEVEL\",\"source\":\"$SOURCE\",\"message\":\"Load test event $i\"}" \
    > /dev/null &

  if (( i % BATCH == 0 )); then
    wait
    sleep "$SLEEP"
    echo "Sent $i / $TOTAL requests..."
  fi
done

wait
echo "Load test completed! Sent $TOTAL log events."
echo "Check Grafana dashboard at http://localhost:3000 for metrics."
