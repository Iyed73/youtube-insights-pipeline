## ─────────────────────────────────────────────────────────────────────────────
## YouTube Trend Analysis Pipeline — developer targets
##
## Prerequisites
##   • Docker running with infra services started:
##       docker compose -f infra/docker-compose.yml up -d kafka schema-registry postgres
##   • .env file present (copy from .env.example and fill in YOUTUBE_API_KEY)
##   • Python 3.11+
##
## Quick start
##   make install          # create venv + install ingestion package
##   make migrate          # create tracked_videos table in Postgres
##   make register-schemas # push Avro schemas to Schema Registry
##   make discover         # run channel discovery once
##   make poll             # run comment poller once
## ─────────────────────────────────────────────────────────────────────────────

SHELL      := /bin/bash
ENV_FILE   ?= .env
VENV       := ingestion/.venv
PY         := $(VENV)/bin/python3
PIP        := $(VENV)/bin/pip
SRC        := ingestion/src

.DEFAULT_GOAL := help

.PHONY: help install migrate register-schemas discover poll

help: ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

# ── Python environment ────────────────────────────────────────────────────────

install: ## Create virtualenv and install ingestion dependencies
	python3 -m venv $(VENV)
	$(PIP) install --quiet --upgrade pip
	$(PIP) install --quiet -e ingestion/
	@echo "✓ Virtualenv ready at $(VENV)"

# ── Infrastructure ────────────────────────────────────────────────────────────

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

# ── Ingestion services ────────────────────────────────────────────────────────

discover: ## Run channel_discovery once: fetch top videos per channel → Kafka
	@set -a && . $(ENV_FILE) && set +a && \
	PYTHONPATH=$(SRC) $(PY) -m channel_discovery.main

poll: ## Run comment_poller once: fetch new comments per active video → Kafka
	@set -a && . $(ENV_FILE) && set +a && \
	PYTHONPATH=$(SRC) $(PY) -m comment_poller.main
