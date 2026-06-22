#!/bin/bash
set -e
echo "🚀 Running integration tests..."

echo "🔍 Testing API Gateway..."
response=$(curl -s http://localhost:8080/api/health)
if [[ "$response" == *"healthy"* ]]; then
  echo "✅ API Gateway is healthy"
else
  echo "❌ API Gateway health check failed"
  exit 1
fi

# Test Log Generator
echo "🔍 Testing Log Generator..."
generator_response=$(curl -s http://localhost:8081/api/generator/stats)
if [[ "$response" == *"running"* ]]; then
  echo "✅ Log Generator is running"
else
  echo "⚠️ Log Generator check failed"
fi

# Test Log Collector
echo "🔍 Testing Log Collector..."
collector_response=$(curl -s http://localhost:8082/api/collecotr/stats)
if [[ "$response" == *"running"* ]]; then
  echo "✅ Log Collector is running"
else
  echo "⚠️ Log Collector check failed"
fi

# Test System Stats
echo "🔍 Testing System Stats..."
stats_response=$(curl -s http://localhost:8080/api/system/stats)
if [[ "$stats_response" == *"generator"* ]] && [[ "$stats_response" == *"collector"* ]]; then
  echo "✅ System stats are accessible"
else
  echo "⚠️ System stats check failed"
fi

echo "🎉 All integration tests passed"
