# Setup Guide

---

## Project Structure

| Folder | Language | Purpose |
|--------|----------|---------|
| `ingestion/` | Python | YouTube API pollers and Kafka Avro producers |
| `streaming/` | Java (Flink) | Near-real-time comment sentiment analysis |
| `batch/` | Python (PySpark) | Deep batch analysis jobs |
| `orchestration/` | Python (Airflow) | DAGs and plugins for pipeline scheduling |
| `infra/` | Docker Compose | All infrastructure services (Kafka, Postgres, Flink, Spark, ClickHouse, Airflow, MinIO) |
| `schemas/` | Avro | Kafka topic schemas (`channel-discovery`, `raw-comments`). Avro is a compact binary serialization format with a schema — it enforces message structure and keeps Kafka payloads small. |
| `models/` | ONNX | Exported ML models used by the Flink streaming job (not committed — generated locally via `optimum-cli`) |
| `config/` | YAML | Channel list and pipeline settings |
| `scripts/` | Python | Utility scripts (e.g. Avro schema registration) |
| `shared/` | — | Shared models (reserved for future cross-project types) |
| `docs/` | Markdown | Documentation |

---

## Setup

### 1. Prerequisites

- Docker running
- Python 3.11–3.13 (3.14 cannot build the `tokenizers` wheel needed for ONNX model export)
- Java 11+ and Maven 3.8+
- `.env` file at the repo root (copy from `.env.example` and fill in `YOUTUBE_API_KEY`)

```bash
cp .env.example .env
# edit .env and set YOUTUBE_API_KEY
```

### 2. Install Python dependencies

```bash
make install
```

> `ingestion/` and `streaming/` are ready. `batch/` and `orchestration/` are not yet functional.
> Run `make help` to see all available targets.

### 3. Start infrastructure

```bash
docker compose -f infra/docker-compose.yml up -d kafka schema-registry postgres
```

Minimum required services for local ingestion. Full stack (Flink, Spark, ClickHouse, Airflow, MinIO):

```bash
docker compose -f infra/docker-compose.yml up -d
```

**Service port map (host):**

| Port | Service |
|------|---------|
| 8080 | Kafka UI |
| 8081 | Schema Registry |
| 8082 | Flink Web UI |
| 8083 | Spark Master Web UI |
| 8085 | Airflow Web UI |
| 8123 | ClickHouse HTTP |
| 9000 | ClickHouse Native TCP |
| 9001 | MinIO Web Console |
| 9002 | MinIO S3 API |
| 9094 | Kafka (external/host) |
| 5432 | PostgreSQL |

> **Note:** MinIO's S3 API is on port **9002** on the host (not 9000) because ClickHouse already occupies 9000.

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

### 7. Export the RoBERTa ONNX model (one-time)

Required for the Flink sentiment job. Must be run with Python 3.11–3.13.

```bash
make export-model
```

Produces `models/twitter-roberta-sentiment/model.onnx` and `tokenizer.json`. These files are gitignored.

### 8. Build the Flink streaming JAR

```bash
make build-streaming
```

Output: `streaming/target/streaming-0.1.0.jar`

### 9. Submit the Flink job

```bash
make submit-job
```

Monitor at **http://localhost:8082** (Flink Web UI).

---

## Running the Pipeline

### Sentiment streaming job

Consumes `raw-comments` from Kafka, runs RoBERTa sentiment analysis, and writes results to the `comments` table in ClickHouse. Submit once — Flink keeps it running continuously.

```bash
# Flink Web UI: http://localhost:8082
# ClickHouse query example (satisfaction per channel, last hour):
docker exec -it clickhouse clickhouse-client --user admin --password admin --query "
SELECT
    channel_id,
    round(countIf(sentiment = 'Positive') / count() * 100, 1) AS satisfaction_pct,
    count() AS total_comments
FROM comments
WHERE processed_at >= now() - INTERVAL 1 HOUR
GROUP BY channel_id
ORDER BY satisfaction_pct DESC;"
```

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

### Video downloader

Downloads the top-satisfaction videos (highest positive-sentiment %) from the last `DOWNLOAD_LOOKBACK_DAYS` days into MinIO for Spark batch processing.

```bash
make download-videos
```

**How it works:**
1. Queries ClickHouse for videos ranked by `positive_comments / total_comments` over the lookback window
2. Skips any video already recorded as `completed` in the `downloaded_videos` PostgreSQL table (redundancy guard)
3. Downloads each video with yt-dlp (up to `VIDEO_MAX_HEIGHT` resolution)
4. Uploads to MinIO at `{MINIO_VIDEOS_BUCKET}/{channel_id}/{video_id}.mp4`
5. Records the result (completed or failed) in PostgreSQL

**Relevant env vars** (in `.env`):

| Variable | Default | Description |
|----------|---------|-------------|
| `DOWNLOAD_LOOKBACK_DAYS` | `7` | Days to look back for satisfaction calculation |
| `TOP_VIDEOS_TO_DOWNLOAD` | `5` | Number of top videos to download per run |
| `MIN_COMMENTS_FOR_DOWNLOAD` | `50` | Minimum comments required to qualify |
| `VIDEO_MAX_HEIGHT` | `720` | Max resolution (e.g. `720`, `1080`, `1440`, `2160`) |
| `MINIO_ENDPOINT` | `minio:9000` | Use `localhost:9002` when running on the host |
| `MINIO_ACCESS_KEY` | `minioadmin` | MinIO access key |
| `MINIO_SECRET_KEY` | `minioadmin` | MinIO secret key |
| `MINIO_VIDEOS_BUCKET` | `videos` | Bucket name for downloaded videos |

> **Host vs Docker:** When running `make download-videos` on your host machine, set `MINIO_ENDPOINT=localhost:9002` and `CLICKHOUSE_HOST=localhost` in `.env`. The defaults (`minio:9000`, `clickhouse`) only work inside the Docker network.

Browse uploaded videos at **http://localhost:9001** (MinIO Web Console, login: `minioadmin` / `minioadmin`).

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
