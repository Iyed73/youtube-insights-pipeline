# Batch Processing Design

## Overview

The batch pipeline processes **top-satisfaction videos** (downloaded via `make download-videos`) to extract transcripts and run topic modeling. It answers: *"What are the most-loved videos actually talking about?"*

The batch layer complements the streaming layer — streaming does real-time sentiment on all tracked videos, batch does deep content analysis on a curated high-performing subset.

---

## Why Only Top-Satisfaction Videos?

- Videos are large — downloading everything is wasteful
- Streaming already provides sentiment data for ALL tracked videos
- The batch layer serves a different purpose: deep analysis on high-performing content
- The resulting topic model is intentionally biased toward positive content, which is actually a more interesting question: *"What topics make audiences happy?"*

---

## Pipeline Steps

```
make download-videos          # downloads top-satisfaction videos to MinIO
make run-batch                # triggers Spark pipeline

Spark Pipeline:
  Postgres (downloaded_videos)
      |
  MinIO (check cached transcripts)
      |
  FFmpeg (audio extraction)      <- only new videos
      |
  Whisper (transcription)        <- only new videos, cache result to MinIO
      |
  All transcripts combined
      |
  Spark ML: Tokenize -> StopWordsRemover -> CountVectorizer -> LDA
      |
  ClickHouse: video_topics + topic_words
      |
  Dashboard: topic x sentiment heatmap, per-channel topics, top words
```

### Step 1: Query Videos to Process
- Read `downloaded_videos` table from Postgres (status = `completed`)
- Get MinIO paths, channel info, satisfaction scores
- This becomes the input DataFrame

### Step 2: Check for Cached Transcripts
- For each video, check if a transcript already exists in MinIO (e.g. `s3a://videos/{channel_id}/{video_id}.txt`)
- Split into two sets: **needs transcription** vs **already transcribed**

### Step 3: Extract Audio (new videos only)
- Use `mapPartitions` to run FFmpeg on each Spark worker
- `ffmpeg -i video.mp4 -vn -ar 16000 -ac 1 -f wav audio.wav`
- 16kHz mono WAV is what Whisper expects

### Step 4: Transcribe with Whisper (new videos only)
- Use `mapPartitions` — load `faster-whisper` model once per partition
- Use `base` or `small` model (good speed/accuracy tradeoff on CPU)
- Save transcripts back to MinIO for caching

### Step 5: Build Full Transcript DataFrame
- Combine cached transcripts + freshly generated ones
- Schema: `[video_id, channel_id, channel_name, satisfaction_pct, transcript]`

### Step 6: Text Preprocessing (Spark ML Pipeline)
```
RegexTokenizer -> StopWordsRemover -> CountVectorizer
```

### Step 7: Train LDA
- `LDA(k=5, maxIter=30, optimizer="online")` — tune k over time
- Fit on full vectorized DataFrame (all transcripts, every run)
- Extract: topic-word distributions + document-topic distributions

### Step 8: Write Results to ClickHouse
- Truncate old run or insert with new `run_id` (see versioning section)
- Write to `analytics.video_topics` and `analytics.topic_words`

---

## Reprocessing All Data Every Run

**Yes, retrain LDA on all transcripts every run.** Reasons:

- LDA is a global model — it discovers topics across the entire corpus
- Running only on new videos produces a different, worse model each time (topic IDs shift)
- Topic comparison across batches becomes meaningless without a stable full corpus

The expensive step (Whisper transcription) is idempotent — transcripts are cached in MinIO and skipped for already-processed videos. Only new videos get transcribed. LDA retraining on all text is fast.

---

## Versioned Runs

Each run gets a `run_id` (UUID or timestamp) stored with all output rows. This enables:

- **Topic drift tracking:** see how top-satisfaction content themes shift over time
- **Model quality auditing:** compare topic coherence across runs
- **Reproducibility:** diagnose what changed between run N and N-1
- **Full lineage:** mature pipeline design for a big data project

**Performance impact is negligible.** Each run produces ~100-200 rows in `video_topics` and ~50-100 rows in `topic_words`. After a year of weekly runs (~52 runs), you'd have ~10,000 rows total — nothing for ClickHouse.

Dashboard queries filter to the latest run:
```sql
WHERE run_id = (SELECT run_id FROM analytics.video_topics ORDER BY run_date DESC LIMIT 1)
```

---

## ClickHouse Schema

### Existing (streaming layer)
```
analytics.comments          <- Flink writes sentiment here (real-time)
```

### New (batch layer)

```sql
CREATE TABLE analytics.video_topics (
    run_id          String,
    run_date        DateTime,
    video_id        String,
    channel_id      LowCardinality(String),
    channel_name    LowCardinality(String),
    topic_id        UInt8,
    topic_weight    Float32,
    dominant_topic  UInt8,
    satisfaction_pct Float32
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(run_date)
ORDER BY (run_id, video_id, topic_id);
```

```sql
CREATE TABLE analytics.topic_words (
    run_id      String,
    run_date    DateTime,
    topic_id    UInt8,
    word        String,
    weight      Float32
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(run_date)
ORDER BY (run_id, topic_id, weight);
```

---

## Dashboard Panels

### Panel 1: Topic Distribution Per Channel (Batch)
Stacked bar chart showing each channel's topic breakdown.

### Panel 2: Topic Treemap / Bubble Chart (Batch)
All topics sized by how many videos fall into each.

### Panel 3: Topic x Sentiment Heatmap (Batch + Streaming Join)
The most valuable view — joins batch topics with streaming sentiment:

```sql
SELECT
    vt.dominant_topic,
    tw.word,
    avg(c.sentiment = 'Positive') AS avg_positive_rate
FROM analytics.video_topics vt
JOIN analytics.topic_words tw ON vt.dominant_topic = tw.topic_id
JOIN analytics.comments c ON vt.video_id = c.video_id
WHERE vt.run_id = (SELECT run_id FROM analytics.video_topics ORDER BY run_date DESC LIMIT 1)
GROUP BY vt.dominant_topic, tw.word
ORDER BY avg_positive_rate DESC
```

Answers: *"Which content topics generate the most positive audience reactions?"*

### Panel 4: Top Words Per Topic (Batch)
Word cloud or horizontal bar chart per topic.

### Panel 5: Channel Topic Evolution Over Time (Batch)
Shows how a channel's topic mix shifts across versioned batch runs.

### Panel 6: Video Detail View (Batch + Streaming)
Per-video: dominant topic, topic distribution, sentiment score, transcript snippet.

---

## Technical Implementation

### Spark Cluster Setup

This project uses **Spark Standalone** mode — Spark's built-in cluster manager — running inside Docker:

```
spark-master  →  accepts spark-submit, coordinates scheduling
spark-worker  →  executes tasks (2 cores, 2GB RAM each)
```

The job is submitted from inside the `spark-master` container:
```bash
spark-submit --master spark://spark-master:7077 /opt/spark/work/batch/jobs/topic_modeling.py
```

The batch source code is mounted as a read-only volume into both containers (`../batch/src:/opt/spark/work/batch:ro`), so code changes take effect immediately without rebuilding the image.

Why Standalone over alternatives:

| Option | Reason not used |
|--------|----------------|
| YARN (Hadoop) | Requires Hadoop ecosystem; data is in MinIO (S3), not HDFS |
| Kubernetes | Production-grade but overkill for local dev |
| Mesos | Deprecated and removed in Spark 4.0 |
| **Standalone** | Self-contained, zero extra dependencies — correct for local dev |

---

### RDDs vs DataFrames — Where Each is Used

#### RDD — Transcription stage (`utils/transcribe.py`)

```python
rdd = spark.sparkContext.parallelize(video_rows)   # distribute video list to workers
transcript_rdd = rdd.mapPartitions(self)            # run Whisper on each partition
return spark.createDataFrame(transcript_rdd)        # convert results back to DataFrame
```

**Why RDD here?**

Whisper transcription is arbitrary Python that:
- Loads a ~150MB ML model into memory
- Downloads binary files from MinIO
- Writes to a temp file on disk

DataFrames are for SQL-style columnar operations. They cannot run arbitrary side-effecting Python code or control when expensive resources are initialized.

**Why `mapPartitions` instead of `map`?**

- `map`: one function call per row → Whisper model loaded **once per video** (wasteful — ~10 seconds of model loading time per video)
- `mapPartitions`: one function call per partition → Whisper model loaded **once per partition**, shared across all videos assigned to that worker

`TranscriptionStage` is designed as a picklable callable:
- Stores only plain config values (strings) — no open connections, no model objects
- Implements `__call__(rows)` so it works directly with `mapPartitions`
- MinIO client and Whisper model are created fresh inside the worker (can't pickle live connections)

#### DataFrame — LDA pipeline (`utils/topic_model.py`)

```python
tokenized  = tokenizer.transform(transcript_df)   # DataFrame in, DataFrame out
filtered   = remover.transform(tokenized)
cv_model   = vectorizer.fit(filtered)
vectorized = cv_model.transform(filtered)
lda_model  = lda.fit(vectorized)
transformed = lda_model.transform(vectorized)
```

**Why DataFrame here?**

Spark ML exclusively uses DataFrames. They provide:
- Optimised columnar execution via Catalyst query planner
- Automatic distribution of ML fitting/transform operations
- Schema enforcement

---

### What Runs Where

| Operation | Driver or Workers | Distributed? |
|-----------|------------------|-------------|
| Postgres query (`VideoRepository`) | Driver | No |
| `parallelize(video_rows)` | Sets up distribution | Creates partitions |
| `mapPartitions` — Whisper transcription | **Workers** | **Yes** |
| `createDataFrame(transcript_rdd)` | Workers hold data | Yes |
| Tokenize → StopWords → CountVectorize | **Workers** | **Yes** |
| LDA fitting and transform | **Workers** | **Yes** |
| `lda_model.describeTopics().collect()` | Workers compute, driver collects | Partially |
| `transformed_df.select(...).collect()` | Workers compute, driver collects | Partially |
| Claude API labeling | Driver | No |
| ClickHouse writes | Driver | No |

---

### Spark ML Pipeline Explained

The four stages in `TopicModelingStage.run()`:

**1. `RegexTokenizer`**
Splits transcript text on non-word characters (`\W+`), drops tokens shorter than 3 characters.
```
"Hello, World! I love this." → ["hello", "world", "love", "this"]
```

**2. `StopWordsRemover`**
Removes common English words using Spark's built-in stop word list (the, is, at, a, I, ...).
```
["hello", "world", "love", "this"] → ["hello", "world", "love"]
```

**3. `CountVectorizer`**
Builds a vocabulary of the top 5000 most frequent words that appear in at least 2 documents (`minDF=2.0`). Converts each document into a sparse vector of word counts.
- `vocabSize=5000` — limits vocabulary size
- `minDF=2.0` — word must appear in ≥2 documents to count (important: with only a few videos this aggressively filters vocabulary)

**4. `LDA` (Latent Dirichlet Allocation)**
Probabilistic model that discovers `k` topics. Each topic is a distribution over words; each document gets a distribution over topics.
- `k=lda_max_topics` — number of topics (default 10)
- `maxIter=20` — EM algorithm iterations
- `seed=42` — reproducible results
- Output: `topicDistribution` column added to each row — a vector like `[0.05, 0.72, 0.08, 0.10, 0.05]`

---

### Transcript Caching

After a video is transcribed, the text is saved to MinIO at `{channel_id}/{video_id}.txt`. On subsequent `make run-batch` calls, the worker reads this cached transcript and skips Whisper entirely. This makes re-runs fast for already-processed videos.

```python
# Check for cached transcript first
transcript = self._get_cached(client, key)
if transcript is not None:
    print(f"  [cached] {row.video_id}")
    yield self._make_row(row, transcript)
    continue

# Only reach here if no cache
transcript = self._transcribe(client, model, row.minio_path)
self._cache(client, key, transcript)       # save to MinIO for next time
```

---

### Topic Labeling with Claude

After LDA runs, the top 10 words per topic are sent to Claude Haiku in a **single API call**:

```
Topic 0: another, second, keep, find, gone
Topic 1: better, plan, held, paying, protocol
Topic 2: little, dangerous, low, see, know
```

Claude returns one short label per topic. Haiku (`claude-haiku-4-5-20251001`) is used because:
- Cheapest and fastest Claude model ($1/$5 per million tokens)
- Simple classification task — no deep reasoning needed

If `ANTHROPIC_API_KEY` is not set, fallback labels `"Topic 0"`, `"Topic 1"` are used automatically.

---

### Whisper Model Pre-baking

The Whisper model is downloaded during the Docker image build (not at runtime):

```dockerfile
ENV HF_HOME=/opt/spark/hf_cache
RUN python3 -c "from faster_whisper import WhisperModel; WhisperModel('base', device='cpu', compute_type='int8')"
RUN chmod -R a+rX /opt/spark/hf_cache
```

`HF_HOME` is set as a Docker `ENV` so it's available both at build time and at runtime when the Spark worker executes. Without this, `huggingface_hub` tries to write to the `spark` user's home (`/nonexistent`) and fails with a permission error.

---

### ClickHouse Schema (Updated)

**`analytics.video_topics`** — one row per (video × topic) combination:

| Column | Type | Description |
|--------|------|-------------|
| `run_id` | String | UUID for this batch run |
| `run_date` | DateTime | When the run happened |
| `video_id` | String | YouTube video ID |
| `channel_id` | LowCardinality(String) | Channel identifier |
| `channel_name` | LowCardinality(String) | Human-readable name |
| `topic_id` | UInt8 | Topic number (0 to k-1) |
| `topic_label` | String | Claude-generated label |
| `topic_weight` | Float32 | Probability this video belongs to topic |
| `dominant_topic` | UInt8 | Topic with highest weight for this video |
| `satisfaction_pct` | Float32 | % positive comments from streaming pipeline |

**`analytics.topic_words`** — one row per (topic × word):

| Column | Type | Description |
|--------|------|-------------|
| `run_id` | String | UUID for this batch run |
| `run_date` | DateTime | When the run happened |
| `topic_id` | UInt8 | Topic number |
| `topic_label` | String | Claude-generated label |
| `word` | String | Word in this topic |
| `weight` | Float32 | Word's probability weight in this topic |

---

### Configuration Reference

All config is read from environment variables in `BatchConfig`:

| Env var | Default | Description |
|---------|---------|-------------|
| `WHISPER_MODEL` | `base` | Whisper model size (base/small/medium/large) |
| `LDA_MAX_TOPICS` | `10` | Max number of LDA topics |
| `LDA_MAX_ITER` | `20` | LDA EM iterations |
| `ANTHROPIC_API_KEY` | `` | Claude API key for topic labeling |
| `VIDEO_MAX_DURATION_SEC` | `600` | Max seconds to download per video (10 min) |
| `SPARK_MASTER` | `spark://spark-master:7077` | Spark master URL |

---

### Monitoring During a Run

```bash
# Watch Whisper progress on workers (prints [cached] / [transcribe] logs)
docker compose -f infra/docker-compose.yml logs -f spark-worker

# Spark Master UI — running applications, executors, task breakdown
open http://localhost:8083

# View results after run
docker exec clickhouse clickhouse-client --query "
    SELECT video_id, topic_label, dominant_topic, satisfaction_pct
    FROM analytics.video_topics
    WHERE run_id = (SELECT run_id FROM analytics.video_topics ORDER BY run_date DESC LIMIT 1)
    GROUP BY video_id, topic_label, dominant_topic, satisfaction_pct
    ORDER BY satisfaction_pct DESC
"
```

---

## No Vector Database Needed

Vector DBs solve similarity search over embeddings. This pipeline does not:
- Perform semantic search
- Do nearest-neighbor retrieval
- Store embeddings for later querying

LDA produces topic distributions (simple arrays like `[0.6, 0.2, 0.1, 0.1]`) — just numbers stored as regular columns.

---

## No Additional Database Needed

ClickHouse handles both streaming and batch results. Having both in the same database enables simple JOINs between sentiment data and topic data with no cross-database complexity.

| Database | Purpose |
|----------|---------|
| **ClickHouse** | All analytics (streaming sentiment + batch topics) |
| **PostgreSQL** | Operational/state DB (tracked channels, downloaded videos status) |
| **MinIO** | Object storage (raw videos + cached transcripts) |

---

## Key Value Proposition

The real insight comes from combining both layers:

| Layer | What it tells you |
|-------|------------------|
| Streaming (sentiment) | "People love this video" |
| Batch (topics) | "This video is about Python tutorials" |
| **Both joined** | **"Python tutorial videos get 72% positive sentiment vs 41% for news commentary"** |

This answers a genuinely useful content strategy question: *"What kind of content makes audiences happiest?"*
