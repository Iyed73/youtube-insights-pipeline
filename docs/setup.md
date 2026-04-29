# Setup Guide

---

## Project Structure

| Folder | Language | Purpose |
|--------|----------|---------|
| `ingestion/` | Python | YouTube API pollers and Kafka Avro producers |
| `streaming/` | Java (Flink) | Near-real-time comment sentiment analysis |
| `batch/` | Python (PySpark) | Deep batch analysis jobs |
| `orchestration/` | Python (Airflow) | DAGs and plugins for pipeline scheduling |
| `infra/` | Docker Compose | All infrastructure services (Kafka, Postgres, Flink, Spark, ClickHouse, Airflow) |
| `schemas/` | Avro | Kafka topic schemas (`channel-discovery`, `raw-comments`). Avro is a compact binary serialization format with a schema — it enforces message structure and keeps Kafka payloads small. |
| `config/` | YAML | Channel list and pipeline settings |
| `scripts/` | Python | Utility scripts (e.g. Avro schema registration) |
| `shared/` | — | Shared models (reserved for future cross-project types) |
| `docs/` | Markdown | Documentation |

---

## Setup

### 1. Prerequisites

- Docker running
- Python 3.11+
- `.env` file at the repo root (copy from `.env.example` and fill in `YOUTUBE_API_KEY`)

```bash
cp .env.example .env
# edit .env and set YOUTUBE_API_KEY
```

### 2. Install Python dependencies

```bash
make install
```

> Only `ingestion/` is ready. `batch/` and `orchestration/` are not yet functional.

### 3. Start infrastructure

```bash
docker compose -f infra/docker-compose.yml up -d kafka schema-registry postgres
```

Minimum required services for local ingestion. Full stack (Flink, Spark, ClickHouse, Airflow):

```bash
docker compose -f infra/docker-compose.yml up -d
```

### 4. Apply database migrations

```bash
make migrate
```

### 5. Create Kafka topics

```bash
make create-topics
```

### 6. Register Avro schemas

```bash
make register-schemas
```

---

## Running the Pipeline

### Channel discovery

Fetches the top videos (by view count) for each channel in `config/channels.yaml` and publishes them to Kafka.

```bash
make discover
```

### Comment poller

Fetches new comments for all active tracked videos and publishes them to Kafka.

```bash
make poll
```

Run these manually or on a schedule. `discover` should run less frequently (e.g. daily); `poll` can run every few minutes.

---

## Configuring Channels

Edit `config/channels.yaml` to control which YouTube channels are tracked:

```yaml
channels:
  - id: UCsBjURrPoezykLs9EqgamOA
    name: Fireship
```

**`id`** — the YouTube channel ID (24 characters, starts with `UC`).
**`name`** — a label used in logs and Kafka messages.

**How to find a channel ID:**
- Go to the channel page on YouTube → click **More** → **About** → share icon → **Copy channel ID**
- Or take it from the URL: `youtube.com/channel/<CHANNEL_ID>`
- Helper tool: https://commentpicker.com/youtube-channel-id.php

The pipeline picks up changes on the next `make discover` run — no restart needed.
