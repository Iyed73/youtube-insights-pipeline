# Architecture

## Overview

A two-layer YouTube analytics pipeline: a **stream path** for near-real-time comment sentiment, and a **batch path** for deep topic analysis on the highest-performing videos. Both layers write to ClickHouse, enabling cross-layer queries that answer *"which content topics generate the most positive audience reactions?"*

---

## Data Flow

```
YouTube API
    │
    ├─▶ Channel Discovery (Python)
    │       Fetches top videos per channel → Kafka: channel-discovery
    │       Upserts into PostgreSQL: tracked_videos
    │
    ├─▶ Comment Poller (Python)
    │       Polls new comments per active video → Kafka: raw-comments
    │       Tracks cursor in PostgreSQL: tracked_videos.comment_cursor
    │
    │       Kafka: raw-comments
    │           │
    │           ▼
    │       Flink (Java) — CommentSentimentJob
    │           RoBERTa sentiment analysis (ONNX Runtime)
    │           → ClickHouse: analytics.comments   [stream path]
    │
    └─▶ Video Downloader (Python)
            Queries ClickHouse for top-satisfaction videos
            Downloads via yt-dlp → MinIO: videos/{channel_id}/{video_id}.mp4
            Records in PostgreSQL: downloaded_videos

                MinIO: videos/
                    │
                    ▼
                Spark (Python) — TopicModelingJob        [batch path]
                    TranscriptionStage (mapPartitions)
                        faster-whisper → MinIO: {channel_id}/{video_id}.txt (cached)
                    TopicModelingStage
                        RegexTokenizer → StopWordsRemover → CountVectorizer → LDA
                    → ClickHouse: analytics.video_topics
                    → ClickHouse: analytics.topic_words
```

### Stream path (near-real-time)

Comments flow from YouTube → Kafka → Flink within seconds. Flink applies a fine-tuned RoBERTa model (cardiffnlp/twitter-roberta-base-sentiment) via ONNX Runtime and writes enriched comments to ClickHouse. The sentiment job runs continuously with 30-second checkpointing.

### Batch path (deep analysis)

The batch path runs on demand (or on a schedule). It processes only the top-satisfaction videos — those with the highest positive-sentiment percentage — to answer *"what are the most-loved videos actually talking about?"*

Transcripts are cached in MinIO so Whisper only runs on new videos. LDA retrains on the full corpus every run to produce stable, comparable topic IDs across batches. Each run is versioned with a `run_id` so topic drift can be tracked over time.

---

## Components

| Component | Technology | Role |
|-----------|-----------|------|
| Channel Discovery | Python | Polls YouTube API for top videos per channel, publishes to Kafka |
| Comment Poller | Python | Polls new comments per video, publishes to Kafka |
| Video Downloader | Python | Downloads top-satisfaction videos to MinIO |
| Flink Sentiment Job | Java / Apache Flink 1.18 | Consumes comments from Kafka, runs RoBERTa sentiment analysis, writes to ClickHouse |
| Spark Batch Job | Python / Apache Spark 3.5 | Transcribes videos with Whisper, runs LDA topic modeling, writes to ClickHouse |
| Airflow | Apache Airflow 2.9 | Orchestrates ingestion schedules and batch job submission |
| Kafka | Apache Kafka 3.9 (KRaft) | Message bus between ingestion and streaming layers |
| Schema Registry | Apicurio Registry | Stores and serves Avro schemas for Kafka topics |

---

## Storage Layers

| Store | Engine | Purpose |
|-------|--------|---------|
| **ClickHouse** | Columnar OLAP | All analytics — streaming sentiment (`analytics.comments`) and batch topics (`analytics.video_topics`, `analytics.topic_words`) |
| **PostgreSQL** | Relational | Operational state — tracked videos, comment cursors, download history, Airflow metadata |
| **MinIO** | S3-compatible object store | Raw video files (`{channel_id}/{video_id}.mp4`) and cached Whisper transcripts (`{channel_id}/{video_id}.txt`) |
| **Kafka** | Distributed log | In-flight messages between ingestion and Flink — `channel-discovery` and `raw-comments` topics |

### ClickHouse tables

```
analytics.comments        — stream path (Flink writes in real time)
    comment_id, video_id, channel_id, channel_name, text,
    sentiment, published_at, processed_at
    Engine: ReplacingMergeTree(processed_at)

analytics.video_topics    — batch path (Spark writes per run)
    run_id, run_date, video_id, channel_id, channel_name,
    topic_id, topic_weight, dominant_topic, satisfaction_pct
    Engine: MergeTree, partitioned by month

analytics.topic_words     — batch path (Spark writes per run)
    run_id, run_date, topic_id, word, weight
    Engine: MergeTree, partitioned by month
```

### Cross-layer query example

```sql
-- Which content topics generate the most positive audience reactions?
SELECT
    vt.dominant_topic,
    tw.word,
    round(avg(c.sentiment = 'Positive') * 100, 1) AS positive_pct
FROM analytics.video_topics vt
JOIN analytics.topic_words tw ON vt.dominant_topic = tw.topic_id
    AND vt.run_id = tw.run_id
JOIN analytics.comments c ON vt.video_id = c.video_id
WHERE vt.run_id = (
    SELECT run_id FROM analytics.video_topics ORDER BY run_date DESC LIMIT 1
)
GROUP BY vt.dominant_topic, tw.word
ORDER BY positive_pct DESC
```
