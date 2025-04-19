#!/usr/bin/env bash
set -e

# Elasticsearch Rally Test Script for KSEA data
COMPOSE_FILE="docker-compose.yml"

# Start ES container
docker-compose -f "$COMPOSE_FILE" up -d elasticsearch

# Wait for ES to be available
echo "Waiting for Elasticsearch to start..."
until curl -s http://localhost:9200/_cluster/health?wait_for_status=yellow > /dev/null; do
  sleep 1
done

# Prepare Rally track
TRACK_PATH="rally-tracks/ksea"
DATA_SRC="../data/training_init.csv"
DATA_DIR="$TRACK_PATH"
mkdir -p "$DATA_DIR"
mkdir -p "$DATA_DIR/results"
mkdir -p "$DATA_DIR/mappings"

# Create index mapping file
cat > "$DATA_DIR/mappings/tweets.json" <<EOF
{
  "settings": {
    "index.number_of_shards": 1,
    "index.number_of_replicas": 0
  },
  "mappings": {
    "properties": {
      "text": { "type": "text" },
      "target": { "type": "keyword" }
    }
  }
}
EOF

echo "Converting CSV to JSONL for Rally..."
python3 - <<EOF
import csv, json, os
with open(os.path.join("$DATA_SRC")) as f:
    reader = csv.DictReader(f)
    with open(os.path.join("$DATA_DIR","training.jsonl"),"w") as out:
        for row in reader:
            out.write(json.dumps(row) + "\n")
EOF

# Function to check if index exists
check_index_exists() {
    curl -s -f "localhost:9200/tweets" > /dev/null 2>&1
    return $?
}

# Benchmark indexing and query at different record counts
RECORD_COUNTS=(1000 10000 100000 1000000)
for cnt in "${RECORD_COUNTS[@]}"; do
  echo "==> Running Rally for $cnt records"

  # Clean up existing index if it exists
  if check_index_exists; then
      echo "Deleting existing index..."
      curl -X DELETE "localhost:9200/tweets"
      # Wait for index deletion to complete
      while check_index_exists; do
          sleep 1
      done
  fi
  sleep 5  # Additional safety wait

  # Slice JSONL - using exact count
  head -n ${cnt} "$DATA_DIR/training.jsonl" > "$DATA_DIR/training_${cnt}.jsonl"
  
  # Get actual line count
  ACTUAL_COUNT=$(wc -l < "$DATA_DIR/training_${cnt}.jsonl")
  echo "Requested ${cnt} records, got ${ACTUAL_COUNT} records"

  # Create per-run track
  TMP_TRACK="rally-tracks/ksea_${cnt}"
  rm -rf "$TMP_TRACK"  # Ensure clean state
  mkdir -p "$TMP_TRACK/mappings"
  cp -r "$DATA_DIR/training_${cnt}.jsonl" "$TMP_TRACK/"
  cp "$DATA_DIR/mappings/tweets.json" "$TMP_TRACK/mappings/"
  
  # Create track.json with actual count
  cat > "$TMP_TRACK/track.json" <<EOF
{
  "version": 2,
  "description": "KSEA tweets benchmark with ${ACTUAL_COUNT} records",
  "corpora": [
    {
      "name": "tweets",
      "documents": [
        {
          "source-file": "training_${cnt}.jsonl",
          "document-count": ${ACTUAL_COUNT}
        }
      ]
    }
  ],
  "indices": [
    {
      "name": "tweets",
      "body": "mappings/tweets.json"
    }
  ],
  "documents": [
    {
      "corpus": "tweets",
      "source-file": "training_${cnt}.jsonl",
      "target-index": "tweets",
      "document-count": ${ACTUAL_COUNT}
    }
  ],
  "challenges": [
    {
      "name": "index",
      "default": true,
      "schedule": [
        {
          "operation": {
            "operation-type": "bulk",
            "bulk-size": 5000
          }
        }
      ]
    },
    {
      "name": "query",
      "schedule": [
        {
          "operation": {
            "operation-type": "search",
            "index": "tweets",
            "body": {
              "query": {
                "match": {
                  "text": "{{default}}"
                }
              }
            }
          }
        }
      ]
    }
  ]
}
EOF

  # Create results directory
  mkdir -p "$TMP_TRACK/results"

  echo "Starting index challenge for $cnt records..."
  if ! docker run --network host \
    -v "${PWD}/${TMP_TRACK}:/rally-tracks/custom" \
    -v "${PWD}/${TMP_TRACK}/results:/rally-tracks/custom/results" \
    elastic/rally race --track-path=/rally-tracks/custom \
    --pipeline=benchmark-only --challenge=index \
    --target-hosts=localhost:9200 --report-format=csv \
    --report-file "/rally-tracks/custom/results/es_rally_index_${cnt}.csv"; then
    echo "Warning: Index challenge failed for $cnt records"
    continue
  fi

  echo "Starting query challenge for $cnt records..."
  if ! docker run --network host \
    -v "${PWD}/${TMP_TRACK}:/rally-tracks/custom" \
    -v "${PWD}/${TMP_TRACK}/results:/rally-tracks/custom/results" \
    elastic/rally race --track-path=/rally-tracks/custom \
    --pipeline=benchmark-only --challenge=query \
    --target-hosts=localhost:9200 --report-format=csv \
    --report-file "/rally-tracks/custom/results/es_rally_query_${cnt}.csv"; then
    echo "Warning: Query challenge failed for $cnt records"
  fi

  # Clean up temp track
  rm -rf "$TMP_TRACK"
  
  # Wait between runs
  sleep 10
done

echo "Elasticsearch Rally tests completed. Reports in results/"
