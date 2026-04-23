# Schemas

This folder holds Avro schema definitions (`.avsc` files) for every Kafka topic in the pipeline. Schemas are registered with Confluent Schema Registry on startup, which enforces compatibility between producers and consumers.

## Naming convention

Files are named `<topic-name>.avsc` and use the namespace `com.yourpipeline`.

## Topics (to be defined)

| Topic | Description | Path |
|---|---|---|
| `channel-discovery` | Channels selected for monitoring | `channel-discovery.avsc` |
| `raw-comments` | Raw comments as polled from YouTube API | `raw-comments.avsc` |
| `sentiment-scores` | Per-comment sentiment output from Flink | `sentiment-scores.avsc` |
| `viral-videos` | Video metadata for videos flagged as viral | `viral-videos.avsc` |
| `batch-analysis-results` | Multi-modal analysis results from Spark | `batch-analysis-results.avsc` |

Schema files will be added alongside the corresponding producer and consumer implementations.
