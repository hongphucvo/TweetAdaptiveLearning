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

# Cleanup function
cleanup() {
    echo "Cleaning up test files..."
    rm -rf "$PAYLOAD_DIR"
    # docker-compose -f "$COMPOSE_FILE" down
}

# Set trap for cleanup on script exit
trap cleanup EXIT

# Test parameters
# Sizes: 1KB to 10GB exponential (1K, 10K, 100K, 1M, 10M, 100M, 1G, 10G)
SIZES=(1024 10240 102400 1048576 10485760 104857600 1073741824 10737418240)
CONC=(1 2)
REQUESTS=1000

echo "Running Spark HTTP throughput tests..."
for size in "${SIZES[@]}"; do
  payload="$PAYLOAD_DIR/payload_${size}.bin"
  if [ ! -f "$payload" ]; then
    echo "  - Generating payload of ${size} bytes ($(numfmt --to=iec-i --suffix=B ${size}))"
    dd if=/dev/urandom of="$payload" bs=1048576 count=$((size/1048576)) status=progress 2>/dev/null
    if [ $((size%1048576)) -ne 0 ]; then
      dd if=/dev/urandom of="$payload" bs=$((size%1048576)) count=1 conv=notrunc oflag=append 2>/dev/null
    fi
  fi
  for c in "${CONC[@]}"; do
    echo "  => size=${size} concurrency=${c}"
    ab -p "$payload" -T application/octet-stream -n $REQUESTS -c $c "$SERVICE_URL" \
      > "$RESULTS_DIR/ab_size${size}_conc${c}.log" 2>&1
  done
done

echo "Spark tests completed. Logs in $RESULTS_DIR"
