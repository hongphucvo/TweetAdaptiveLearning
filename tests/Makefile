# Makefile for running all KSEA tests

.PHONY: all kafka spark es clean

all: kafka spark es

kafka:
	@echo "Running Kafka tests..."
	bash kafka_test.sh

spark:
	@echo "Running Spark tests..."
	bash spark_test.sh

es:
	@echo "Running Elasticsearch tests..."
	docker pull elastic/rally:latest
	bash rally_test.sh

clean:
	@echo "Cleaning test results..."
	rm -rf results/ payloads/ data/
