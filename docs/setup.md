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

---

## Batch Processing Pipeline

The batch processing pipeline uses **Apache Spark** to run parallel, heavy-duty processing of videos and comments. It consists of two layers:
1. **Silver Layer (Scene Detection)**: Extracts visual features from raw MP4 video files stored in MinIO.
2. **Gold Layer (Sentiment Correlation)**: Joins the visual features with aggregated audience sentiment data from ClickHouse, producing correlates (e.g., editing pace vs. audience satisfaction).

This guide walks you through setting up and running the batch pipeline from scratch.

### Step 1: Pre-requisites & Infrastructure Setup

Ensure the infrastructure stack is fully running. If not, spin it up using Docker:

```bash
# Start all containers (Kafka, ClickHouse, MinIO, Postgres, Flink, Spark)
docker compose -f infra/docker-compose.yml up -d
```

Confirm that the Spark cluster is healthy:
- **Spark Master UI**: [http://localhost:8083](http://localhost:8083) (should show 2 registered workers: `spark-worker` and `spark-worker-2`).

#### Environment Configuration

Make sure your `.env` file contains the following **Spark Batching** configuration variables. You can adjust them as needed:

| Variable | Default | Description |
|----------|---------|-------------|
| `NUM_WORKERS` | `2` | Number of Spark worker nodes in the cluster. |
| `CORES_PER_WORKER` | `2` | Number of CPU cores allocated per Spark worker. |
| `MINIO_RESULTS_BUCKET` | `analytics` | Bucket name for storing Silver and Gold results. |
| `SCENE_THRESHOLD` | `3.0` | Sensitivity threshold for PySceneDetect `AdaptiveDetector` (lower = more sensitive). |
| `DOWNSCALE_FACTOR` | `4` | Image downscaling factor for scene detection to reduce worker memory usage (e.g. `4` downscales 720p to 180p). |

### Step 2: Build the Spark Executor Virtual Environment (One-time)

To ensure that PySceneDetect, OpenCV, PyAV, and MinIO packages are available on Spark workers without pre-installing them on the host or the base Docker image, we pack a virtual environment using `venv-pack` and distribute it to workers.

If `batch/environment.tar.gz` is not present, or if you update dependencies, build it by running:

```bash
# Option 1: Use the Makefile helper
make build-venv

# Option 2: Run directly via docker exec (must run as root)
docker exec -u root -it spark-master /opt/spark/batch/build_venv.sh
```

This script:
1. Installs system libraries (FFmpeg/OpenCV dependencies) on the container.
2. Creates a virtual environment.
3. Installs `minio`, `scenedetect`, `av`, `numpy`, `venv-pack`, and `clickhouse-connect`.
4. Packs it into `batch/environment.tar.gz` which is shared across workers via a Docker volume mount.

### Step 3: Populate Raw Videos (Bronze Layer)

The Silver layer analyzes videos that are downloaded into the MinIO `videos` bucket.

1. Ensure channels are configured in `config/channels.yaml`.
2. Discover channels and poll comments (so ClickHouse has sentiment data):
   ```bash
   make discover
   make poll
   ```
3. Run the streaming job to process comments and populate the ClickHouse database:
   ```bash
   make submit-job
   ```
4. Download the videos to MinIO:
   ```bash
   make download-videos
   ```
   *Verify that the videos are downloaded by visiting the MinIO Web Console at [http://localhost:9001](http://localhost:9001) (login: `minioadmin` / `minioadmin`) inside the `videos` bucket.*

### Step 4: Run the Silver Layer Job (Scene Detection)

This job performs distributed scene cutting and video metric analysis.

Run the job using:
```bash
make submit-silver
```

**What happens under the hood:**
1. The driver scans the `videos` bucket in MinIO.
2. It distributes the list of videos to executors (`spark-worker` and `spark-worker-2`) across partitions.
3. Each worker downloads its allocated video locally, opens it via **PyAV**, and runs **PySceneDetect** (using the `AdaptiveDetector` algorithm).
4. The workers extract:
   - `cuts`: Total scene transitions.
   - `duration_sec`: Total runtime.
   - `asl_sec`: Average Shot Length (Duration / Cuts).
   - `cut_density`: Count of cuts per 60-second window (array).
   - `avg_brightness`: Average grayscale value of frame samples.
5. The dataframe is cached on the cluster, and written to the shared directory `/opt/spark/batch/tmp/scene_metrics/` (which maps to `batch/tmp/scene_metrics/` on the host).
6. The driver uploads the resulting Parquet files to the MinIO `analytics` bucket under `scene_metrics/`.

### Step 5: Run the Gold Layer Job (Sentiment Correlation)

The Gold job joins the visual metrics from the Silver layer with the comment sentiment aggregates stored in ClickHouse.

Run the job using:
```bash
make submit-gold
```

**What happens under the hood:**
1. The driver downloads the Silver Parquet files from MinIO to the shared staging directory `/opt/spark/batch/tmp/silver/`.
2. It reads them into a Spark DataFrame and caches them.
3. It fetches comment sentiment aggregates from ClickHouse (`analytics.comments`):
   - Total comment count.
   - Positive/Neutral/Negative sentiment frequencies.
4. Spark performs a `left join` on `video_id` (so videos without comments are still preserved with NULL sentiment fields).
5. It computes derived columns:
   - Sentiment ratios and normalized `sentiment_score` (`(positive - negative) / total`).
   - `editing_pace`: `'fast'` (<3s average shot length), `'medium'` (3–8s), or `'slow'` (>8s).
   - `pace_sentiment_label`: Combines the editing pace and sentiment score to show how users react (e.g., `fast_positive`).
6. Writes the final results to MinIO under `analytics/gold/` as Parquet.
7. Creates the database `gold` and table `gold.video_insights` in ClickHouse (if they do not exist) and inserts the rows.

### Step 6: Verify Gold Insights in ClickHouse

Query the final Gold metrics in ClickHouse to see the correlations:

```bash
docker exec -it clickhouse clickhouse-client --user admin --password admin --query "
SELECT 
    video_id,
    cuts,
    asl_sec,
    editing_pace,
    total_comments,
    sentiment_score,
    pace_sentiment_label
FROM gold.video_insights
LIMIT 10;
"
```
