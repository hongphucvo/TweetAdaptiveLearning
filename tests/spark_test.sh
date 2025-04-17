#!/usr/bin/env bash
set -e

# Spark Test Script
# Uses Apache Bench to execute and track performance over increasing payload sizes and concurrency.
COMPOSE_FILE="docker-compose.yml"

echo "Starting Spark cluster..."
docker-compose -f "$COMPOSE_FILE" up -d spark-master spark-worker

# Service endpoint for Spark ingestion (adjust as needed)
SERVICE_URL=${SERVICE_URL:-http://localhost:8080/ingest}

# Directories for payloads and results
RESULTS_DIR=results/spark
PAYLOAD_DIR=payloads/spark
mkdir -p "$RESULTS_DIR" "$PAYLOAD_DIR"

# Test parameters
# Sizes: 1KB to 10GB exponential
SIZES=(1024 10240 102400 1024000 10240000 102400000 1024000000 10240000000)
CONC=(1 2)
REQUESTS=1000

echo "Running Spark HTTP throughput tests..."
for size in "${SIZES[@]}"; do
  payload="$PAYLOAD_DIR/payload_${size}.bin"
  if [ ! -f "$payload" ]; then
    echo "  - Generating payload of ${size} bytes"
    head -c "$size" </dev/urandom >"$payload"
  fi
  for c in "${CONC[@]}"; do
    echo "  => size=${size} concurrency=${c}"
    ab -p "$payload" -T application/octet-stream -n $REQUESTS -c $c "$SERVICE_URL" \
      > "$RESULTS_DIR/ab_size${size}_conc${c}.log" 2>&1
  done
done

echo "Spark tests completed. Logs in $RESULTS_DIR"
