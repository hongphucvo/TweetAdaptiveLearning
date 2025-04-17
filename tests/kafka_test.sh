#!/usr/bin/env bash
set -e

# Kafka Test Script
# Vary data size (1KB to 1GB exponential) and concurrency (1 to 2)
COMPOSE_FILE="docker-compose.yml"

echo "Starting Kafka cluster..."
docker-compose -f "$COMPOSE_FILE" up -d zookeeper kafka

# Directories for payloads and results
RESULTS_DIR=results/kafka
PAYLOAD_DIR=payloads/kafka
dir -ItemType Directory "$RESULTS_DIR" 2>nul || mkdir -p "$RESULTS_DIR"
dir -ItemType Directory "$PAYLOAD_DIR" 2>nul || mkdir -p "$PAYLOAD_DIR"

# Test parameters
SIZES=(1024 10240 102400 1024000 10240000 102400000 1024000000)
CONC=(1 2)
TOTAL_MSG=1000000
THROUGHPUT=10000

echo "Running producer tests..."
for size in "${SIZES[@]}"; do
  payload="$PAYLOAD_DIR/payload_${size}.bin"
  if [ ! -f "$payload" ]; then
    echo "  - Generating payload of ${size} bytes"
    head -c "$size" </dev/urandom >"$payload"
  fi
  for c in "${CONC[@]}"; do
    echo "  => size=${size} concurrency=${c}"
    for i in $(seq 1 "$c"); do
      docker-compose -f "$COMPOSE_FILE" exec -T kafka kafka-producer-perf-test.sh \
        --topic stress-test-topic \
        --num-records $((TOTAL_MSG/c)) \
        --record-size "$size" \
        --throughput "$THROUGHPUT" \
        --producer-props bootstrap.servers=kafka:29092 \
        > "$RESULTS_DIR/producer_size${size}_conc${c}_${i}.log" 2>&1 &
    done
    wait
  done
done

# Integrity test: consume all messages
echo "Running consumer integrity test..."
docker-compose -f "$COMPOSE_FILE" exec -T kafka kafka-consumer-perf-test.sh \
  --topic stress-test-topic \
  --messages "$TOTAL_MSG" \
  --group integrity-test \
  --bootstrap-server kafka:29092 \
  > "$RESULTS_DIR/consumer_integrity.log" 2>&1

echo "Kafka tests completed. Logs in $RESULTS_DIR"
