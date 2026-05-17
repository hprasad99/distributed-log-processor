#!/bin/bash

echo " Running load test against the log processing system..."

# Wait for services to be ready
echo " Waiting for the services to be ready..."
until curl -f http://localhost:8080/api/logs/health > /dev/null 2>&1; do
  echo "Waiting for the API gateway..."
  sleep 2
done

echo "Services are ready!"

# Generate test data
echo "📊Generating test log events..."

# Simple load test using curl
for i in {1..10000}; do
  curl --request POST \
    --url http://localhost:8080/api/logs \
    --header 'content-type: application/json' \
    --data '{
    "organizationId": "org-$((i%5))",
    "level": "INFO",
    "source": "log-producer",
    "message": "testing log processor"
  }' &
  # limit concurrent requests
  if((i%10 == 0)); then
    wait
    echo "Sent $i requests..."
  fi
done

wait
echo "✅ Load test completed! Sent 100 log events."
echo "📊 Check Grafana dashboard at http://localhost:3000 for metrics."
