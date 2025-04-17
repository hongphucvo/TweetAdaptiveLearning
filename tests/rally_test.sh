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
DATA_SRC="../data/training.csv"
DATA_DIR="$TRACK_PATH/data"
mkdir -p "$DATA_DIR"

echo "Converting CSV to JSONL for Rally..."
python3 - <<EOF
import csv, json, os
with open(os.path.join("$DATA_SRC")) as f:
    reader = csv.DictReader(f)
    with open(os.path.join("$DATA_DIR","training.jsonl"),"w") as out:
        for row in reader:
            out.write(json.dumps(row) + "\n")
EOF

# Benchmark indexing and query at different record counts
RECORD_COUNTS=(1000 10000 100000 1000000)
for cnt in "${RECORD_COUNTS[@]}"; do
  echo "==> Running Rally for $cnt records"

  # Slice JSONL
  # head -n $((cnt+1)) "$DATA_DIR/training.jsonl" > "$DATA_DIR/training_${cnt}.jsonl"

  # Create per-run track
  TMP_TRACK="rally-tracks/ksea_${cnt}"
  rm -rf "$TMP_TRACK"
  cp -r "$TRACK_PATH" "$TMP_TRACK"
  sed -i "s|training.jsonl|training_${cnt}.jsonl|g" "$TMP_TRACK/track.json"

  # Run index challenge
  
  # Run index challenge using Docker
  docker run --network host -v "${PWD}/${TMP_TRACK}:/rally-tracks/custom" \
    elastic/rally race --track-path=/rally-tracks/custom \
    --pipeline=benchmark-only --challenge=index \
    --target-hosts=localhost:9200 --report-format=csv \
    --report-file "/rally-tracks/custom/results/es_rally_index_${cnt}.csv"

  # Run query challenge using Docker
  docker run --network host -v "${PWD}/${TMP_TRACK}:/rally-tracks/custom" \
    elastic/rally race --track-path=/rally-tracks/custom \
    --pipeline=benchmark-only --challenge=query \
    --target-hosts=localhost:9200 --report-format=csv \
    --report-file "/rally-tracks/custom/results/es_rally_query_${cnt}.csv"

  # Clean up temp track
  rm -rf "$TMP_TRACK"
done

echo "Elasticsearch Rally tests completed. Reports in results/"
