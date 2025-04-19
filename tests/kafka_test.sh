#!/bin/bash

# Kafka configuration
KAFKA_CONTAINER="test_kafka"
BOOTSTRAP_SERVERS="kafka:29092"
TOPIC_NAME="kafka-perf-test"
NUM_MESSAGES=100000
MESSAGE_SIZE=1024
NUM_PARTITIONS=3
REPLICATION_FACTOR=1

# Test parameters
RECORD_COUNTS=(1000 10000 100000 1000000)
OUTPUT_DIR="$(dirname "$0")/test_results"
PRODUCER_CSV="${OUTPUT_DIR}/producer_metrics.csv"
CONSUMER_CSV="${OUTPUT_DIR}/consumer_metrics.csv"

# Create output directory
mkdir -p "$OUTPUT_DIR"

# Initialize CSV files with headers
echo "Records,MessageSize,Throughput(records/sec),Latency(ms),TotalTime(ms)" > "$PRODUCER_CSV"
echo "Records,MessageSize,Throughput(records/sec),TotalTime(ms)" > "$CONSUMER_CSV"

# Check and start docker compose services
ensure_services() {
    echo "Checking docker compose services..."
    if ! docker ps | grep -q test_kafka; then
        echo "Starting docker compose services..."
        docker-compose -f "$(dirname "$0")/docker-compose.yml" up -d
        echo "Waiting for services to start..."
        sleep 10
    fi
}

# Create test topic
create_topic() {
    echo "Creating test topic: $TOPIC_NAME"
    if ! docker exec $KAFKA_CONTAINER /usr/bin/kafka-topics \
        --create \
        --bootstrap-server $BOOTSTRAP_SERVERS \
        --topic $TOPIC_NAME \
        --partitions $NUM_PARTITIONS \
        --replication-factor $REPLICATION_FACTOR \
        --if-not-exists; then
        echo "Failed to create topic"
        exit 1
    fi
}

# Parse producer metrics and save to CSV
save_producer_metrics() {
    local records=$1
    local size=$2
    awk -v r="$records" -v s="$size" '
        /records sent.*records\/sec/ {
            split($0, arr, ",")
            throughput = $3
            avg_latency = arr[3]
            max_latency = arr[4]
            gsub(/[^0-9.]/, "", throughput)
            gsub(/[^0-9.]/, "", avg_latency)
            gsub(/[^0-9.]/, "", max_latency)
            if (r == $1) {  # Only save the final summary line for full test
                printf "%d,%d,%.2f,%.2f,%.2f\n", r, s, throughput, avg_latency, max_latency
            }
        }
    ' >> "$PRODUCER_CSV"
}

# Parse consumer metrics and save to CSV
save_consumer_metrics() {
    local records=$1
    local size=$2
    awk -v r="$records" -v s="$size" '
        /fetch.time.ms/ {
            split($0, arr, ",")
            fetch_time = arr[8]
            gsub(/[^0-9.]/, "", fetch_time)
            printf "%d,%d,%.2f,%.2f\n", r, s, 0.00, fetch_time
        }
    ' >> "$CONSUMER_CSV"
}

# Run tests with different parameters
run_performance_tests() {
    local msg_size=$1
    local num_records=$2
    
    echo "Testing with $num_records records of size $msg_size bytes"
    
    # Producer test
    echo "Producer test..."
    docker exec $KAFKA_CONTAINER /usr/bin/kafka-producer-perf-test \
        --topic $TOPIC_NAME \
        --num-records $num_records \
        --record-size $msg_size \
        --throughput -1 \
        --producer-props bootstrap.servers=$BOOTSTRAP_SERVERS 2>&1 | tee >(save_producer_metrics $num_records $msg_size)
    
    sleep 2
    
    # Consumer test
    echo "Consumer test..."
    docker exec $KAFKA_CONTAINER /usr/bin/kafka-consumer-perf-test \
        --bootstrap-server $BOOTSTRAP_SERVERS \
        --topic $TOPIC_NAME \
        --messages $num_records 2>&1 | tee >(save_consumer_metrics $num_records $msg_size)
    
    sleep 2
}

# Cleanup
cleanup() {
    echo "Cleaning up test topic..."
    docker exec $KAFKA_CONTAINER /usr/bin/kafka-topics \
        --delete \
        --bootstrap-server $BOOTSTRAP_SERVERS \
        --topic $TOPIC_NAME
}

# Main execution
echo "Starting Kafka performance tests..."
ensure_services
create_topic

# Run tests with different message counts
for count in "${RECORD_COUNTS[@]}"; do
    run_performance_tests $MESSAGE_SIZE $count
done

cleanup
echo "Performance tests completed. Results saved in ${OUTPUT_DIR}"

# Cleanup function to stop services if needed
cleanup_services() {
    read -p "Do you want to stop docker compose services? (y/n) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        docker-compose -f "$(dirname "$0")/docker-compose.yml" down
    fi
}

cleanup_services
