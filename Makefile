SHELL        := /bin/bash
ENV_FILE     ?= .env
VENV         := ingestion/.venv
PY           := $(VENV)/bin/python3
PIP          := $(VENV)/bin/pip
SRC          := ingestion/src

STREAMING_JAR := streaming/target/streaming-0.1.0.jar
MODEL_DIR     := models/twitter-roberta-sentiment

COMPOSE := docker compose -f infra/docker-compose.yml --env-file $(ENV_FILE)

.DEFAULT_GOAL := help

.PHONY: help install create-topics migrate register-schemas discover poll \
        export-model build-streaming submit-job stop-job \
        download-videos build-airflow build-ingestion build-batch run-batch

help: ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

install: ## Create virtualenv and install ingestion dependencies
	python3 -m venv $(VENV)
	$(PIP) install --quiet --upgrade pip
	$(PIP) install --quiet -e ingestion/
	@echo "✓ Virtualenv ready at $(VENV)"

create-topics: ## Create Kafka topics (must run before discover/poll)
	@set -a && . $(ENV_FILE) && set +a && \
	$(PY) -c "\
from confluent_kafka.admin import AdminClient, NewTopic, ConfigResource; \
a = AdminClient({'bootstrap.servers': '$$KAFKA_BOOTSTRAP_SERVERS'}); \
topics = [ \
  NewTopic('_schemas', 1, 1, config={'cleanup.policy': 'compact'}), \
  NewTopic('channel-discovery', 3, 1), \
  NewTopic('raw-comments', 6, 1), \
]; \
fs = a.create_topics(topics); \
[print(f'  {t}: ' + ('ok' if (e := f.exception()) is None or e.args[0].name == 'TOPIC_ALREADY_EXISTS' else str(e))) for t, f in fs.items()]"
	@echo "✓ Kafka topics ready."

migrate: ## Apply database migrations via Alembic (upgrade to head)
	@set -a && . $(ENV_FILE) && set +a && \
	PYTHONPATH=$(SRC) $(PY) -m alembic -c ingestion/alembic.ini upgrade head
	@echo "✓ Migrations applied."

register-schemas: ## Register Avro schemas with Schema Registry
	@set -a && . $(ENV_FILE) && set +a && \
	PYTHONPATH=$(SRC) $(PY) scripts/register_schemas.py

discover: ## Run channel_discovery once: fetch top videos per channel → Kafka
	@set -a && . $(ENV_FILE) && set +a && \
	PYTHONPATH=$(SRC) $(PY) -m channel_discovery.main

poll: ## Run comment_poller once: fetch new comments per active video → Kafka
	@set -a && . $(ENV_FILE) && set +a && \
	PYTHONPATH=$(SRC) $(PY) -m comment_poller.main

download-videos: ## Download top-satisfaction videos to MinIO (last DOWNLOAD_LOOKBACK_DAYS days)
	@set -a && . $(ENV_FILE) && set +a && \
	PYTHONPATH=$(SRC) $(PY) -m video_downloader.main

export-model: ## Export RoBERTa to ONNX (one-time, requires Python ≤ 3.13)
	@echo "Installing optimum + transformers..."
	$(PIP) install --quiet "optimum[onnxruntime]" transformers
	$(VENV)/bin/optimum-cli export onnx \
		--model cardiffnlp/twitter-roberta-base-sentiment \
		$(MODEL_DIR)/
	@echo "✓ Model exported to $(MODEL_DIR)/"

build-streaming: ## Compile the Flink streaming fat JAR
	cd streaming && mvn clean package -DskipTests -q
	@echo "✓ JAR built at $(STREAMING_JAR)"

submit-job: ## Copy the JAR into Flink and submit the sentiment job
	@if [ ! -f $(STREAMING_JAR) ]; then \
		echo "ERROR: $(STREAMING_JAR) not found — run 'make build-streaming' first"; \
		exit 1; \
	fi
	docker cp $(STREAMING_JAR) flink-jobmanager:/tmp/streaming.jar
	docker exec flink-jobmanager \
		flink run --jobmanager flink-jobmanager:8081 /tmp/streaming.jar
	@echo "✓ Job submitted — monitor at http://localhost:8082"

stop-job: ## Cancel the running sentiment Flink job
	@JOB_ID=$$(docker exec flink-jobmanager flink list 2>/dev/null \
		| grep 'YouTube Comment Sentiment' \
		| awk '{print $$4}'); \
	if [ -z "$$JOB_ID" ]; then \
		echo "No running sentiment job found."; \
	else \
		docker exec flink-jobmanager flink cancel $$JOB_ID && \
		echo "✓ Cancelled job $$JOB_ID"; \
	fi

build-airflow: ## Build custom Airflow image with Docker provider pre-installed
	$(COMPOSE) build airflow-webserver
	@echo "✓ Airflow image built as youtube-insights/airflow:latest"

build-ingestion: ## Build the ingestion image (comment-poller service and ingestion Airflow tasks)
	$(COMPOSE) build comment-poller
	@echo "✓ Ingestion image built as youtube-insights/ingestion:latest"

build-batch: ## Build the Spark image (deps, models, job code) and restart the Spark cluster on it
	$(COMPOSE) build spark-master spark-worker
	$(COMPOSE) up -d spark-master spark-worker
	@echo "✓ Spark image built — spark-master and spark-worker now run the new code"

run-batch: ## Trigger the batch_topic_modeling DAG (transcribe → LDA → labels → ClickHouse)
	docker exec airflow-scheduler airflow dags trigger batch_topic_modeling
	@echo "✓ Triggered — follow it at http://localhost:8085 (the DAG must be unpaused to run)"
