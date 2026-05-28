"""
Phase 2 — Global LDA Topic Modeling
=====================================
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

from pyspark.ml import Pipeline
from pyspark.ml.clustering import LDA
from pyspark.ml.feature import (
    CountVectorizer,
    CountVectorizerModel,
    IDF,
    RegexTokenizer,
    StopWordsRemover,
)
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from config import cfg
from spark_session import get_spark_session
from utils.postgres import read_query, write_table
from utils.watermark import get_watermark, mark_running, set_watermark

# Safely import your isolated UDFs
from utils.topic_udfs import extract_dominant, serialize_distribution

logging.basicConfig(
    level  = logging.INFO,
    format = "%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)

# ── YouTube-domain stop words ─────────────────────────────────────────────────
_DOMAIN_STOPS = [
    "like", "subscribe", "channel", "video", "youtube", "guys",
    "going", "know", "think", "want", "okay", "right", "just",
    "really", "actually", "basically", "literally", "also",
    "um", "uh", "yeah", "hey", "hi", "hello", "watch", "click",
    "comment", "share", "follow", "link", "description", "below",
]

# ── GLOBAL UDF REGISTRATION (Prevents Recursion/Stack Overflow) ───────────────
extract_dominant_udf = F.udf(extract_dominant, T.IntegerType())
ser_dist_udf         = F.udf(serialize_distribution, T.StringType())


# ── Step helpers ──────────────────────────────────────────────────────────────

def _load_transcripts(spark: SparkSession) -> DataFrame:
    query = """
        SELECT video_id,
               transcript_text
        FROM   transcripts
        WHERE  extraction_status = 'success'
          AND  transcript_text   IS NOT NULL
          AND  word_count        >= 20
    """
    df = read_query(spark, query, alias="corpus")
    logger.info("[phase2] Corpus size: %d documents.", df.count())
    return df


def _build_pipeline() -> Pipeline:
    tok = RegexTokenizer(
        inputCol       = "transcript_text",
        outputCol      = "tokens_raw",
        pattern        = r"[^a-zA-Z]+",
        toLowercase    = True,
        minTokenLength = 3,
    )
    swr = StopWordsRemover(
        inputCol  = "tokens_raw",
        outputCol = "tokens",
        stopWords = StopWordsRemover.loadDefaultStopWords("english") + _DOMAIN_STOPS,
    )
    cv = CountVectorizer(
        inputCol  = "tokens",
        outputCol = "raw_features",
        vocabSize = cfg.lda.vocab_size,
        minDF     = float(cfg.lda.min_doc_freq),
        maxDF     = cfg.lda.max_doc_freq,
    )
    idf = IDF(
        inputCol   = "raw_features",
        outputCol  = "features",
        minDocFreq = cfg.lda.min_doc_freq,
    )
    return Pipeline(stages=[tok, swr, cv, idf])


def _train_lda(features_df: DataFrame) -> "LDAModel":
    logger.info(
        "[phase2] Training LDA  k=%d  maxIter=%d  optimizer=online …",
        cfg.lda.num_topics, cfg.lda.max_iter,
    )
    model = LDA(
        k           = cfg.lda.num_topics,
        maxIter     = cfg.lda.max_iter,
        optimizer   = "online",
        featuresCol = "features",
        seed        = 42,
    ).fit(features_df)

    logger.info(
        "[phase2] LDA trained  log-likelihood=%.4f  perplexity=%.4f",
        model.logLikelihood(features_df),
        model.logPerplexity(features_df),
    )
    return model

def _assign_topics(
    features_df: DataFrame,
    lda_model,
    cv_model:    CountVectorizerModel,
    run_id:      str,
    model_path:  str,
) -> DataFrame:
    from pyspark.ml.functions import vector_to_array

    vocab   = cv_model.vocabulary
    k       = cfg.lda.num_topics
    n_terms = cfg.lda.top_terms
    now     = datetime.now(tz=timezone.utc)

    # 1. Native ML Transform
    transformed = lda_model.transform(features_df)

    # 2. Native Vector Parsing (Zero UDFs)
    # Convert the vector to an array, find the max probability, and find its index.
    dist_arr = vector_to_array(F.col("topicDistribution"))
    max_prob = F.array_max(dist_arr)
    # array_position returns a 1-based index; subtract 1 for the true topic ID
    dominant_col = (F.array_position(dist_arr, max_prob).cast(T.IntegerType()) - F.lit(1))
    
    # Cast the full distribution array to a JSON string natively
    json_distribution_col = F.to_json(dist_arr)

    # 3. Build a Native Map Expression for the top terms (Zero DataFrame creation)
    map_elements = []
    for row in lda_model.describeTopics(n_terms).collect():
        topic_id = int(row["topic"])
        terms_json = json.dumps([vocab[i] for i in row["termIndices"]])
        map_elements.append(F.lit(topic_id))
        map_elements.append(F.lit(terms_json))

    # This creates a native Spark SQL map mapping topic_id -> top_terms_json
    native_terms_lookup = F.create_map(*map_elements)

    # 4. Project and map all columns purely in Spark SQL
    return (
        transformed.select(
            F.col("video_id"),
            F.lit(run_id).alias("run_id"),
            dominant_col.alias("dominant_topic_id"),
            json_distribution_col.alias("topic_distribution"),
            F.lit(k).alias("num_topics"),
            F.lit(model_path).alias("model_version"),
            F.lit(now).alias("assigned_at"),
        )
        .withColumn(
            "top_terms",
            F.coalesce(native_terms_lookup.getItem(F.col("dominant_topic_id")), F.lit("[]"))
        )
        .select(
            "video_id", "run_id", "dominant_topic_id", "topic_distribution",
            "top_terms", "num_topics", "model_version", "assigned_at"
        )
    )

def _save_model(lda_model, run_id: str) -> str:
    path = cfg.minio.s3a_path(f"models/lda/{run_id}")
    logger.info("[phase2] Saving LDA model → %s", path)
    try:
        lda_model.save(path)
    except Exception as exc:
        logger.warning("[phase2] Model save failed (non-fatal): %s", exc)
    return path


# ── Entry point ───────────────────────────────────────────────────────────────

def run(spark: SparkSession | None = None) -> None:
    owns_spark = spark is None
    if owns_spark:
        spark = get_spark_session("Phase2_LDATopicModeling")

    run_id = datetime.now(tz=timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    try:
        mark_running(cfg.JOB_LDA)

        corpus_df = _load_transcripts(spark)
        doc_count = corpus_df.count()

        if doc_count < cfg.lda.num_topics:
            logger.warning(
                "[phase2] Only %d docs — need at least %d for k=%d topics. Skipping.",
                doc_count, cfg.lda.num_topics, cfg.lda.num_topics,
            )
            set_watermark(cfg.JOB_LDA, datetime.now(tz=timezone.utc), 0)
            return

        pipeline_model = _build_pipeline().fit(corpus_df)
        features_df    = pipeline_model.transform(corpus_df).cache()
        cv_model       = pipeline_model.stages[2]

        lda_model  = _train_lda(features_df)
        model_path = _save_model(lda_model, run_id)

        topic_df = _assign_topics(features_df, lda_model, cv_model, run_id, model_path).cache()
        features_df.unpersist()

        write_table(topic_df, "video_topics", mode="append")
        rows_processed_count = topic_df.count()

        set_watermark(
            cfg.JOB_LDA,
            new_watermark   = datetime.now(tz=timezone.utc),
            rows_processed  = rows_processed_count,
            status          = "success",
            meta            = {
                "run_id":     run_id,
                "num_topics": cfg.lda.num_topics,
                "doc_count":  doc_count,
                "model_path": model_path,
            },
        )
        logger.info(
            "[phase2] ✓ Phase 2 complete — run_id=%s  %d assignments written.",
            run_id, rows_processed_count,
        )
        topic_df.unpersist()

    except Exception as exc:
        logger.exception("[phase2] Unhandled error: %s", exc)
        set_watermark(
            cfg.JOB_LDA,
            new_watermark   = datetime.now(tz=timezone.utc),
            rows_processed  = 0,
            status          = "failed",
            meta            = {"error": str(exc), "run_id": run_id},
        )
        raise

    finally:
        if owns_spark:
            spark.stop()


if __name__ == "__main__":
    run()