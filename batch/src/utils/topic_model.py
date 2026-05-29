from __future__ import annotations

from dataclasses import dataclass

import numpy as np
from pyspark.ml.clustering import LDA, LDAModel
from pyspark.ml.feature import CountVectorizer, CountVectorizerModel, RegexTokenizer, StopWordsRemover
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType

_NLTK_DATA_DIR = "/tmp/nltk_data"

# Lazily initialised per-process (driver or worker) — avoids module-level download
# which fails when the home dir is /nonexistent in Spark containers.
_lemmatizer = None


def _lemmatize_tokens(tokens: list[str]) -> list[str]:
    """Lemmatize a list of tokens. Downloads wordnet on first call per process."""
    global _lemmatizer
    if _lemmatizer is None:
        import nltk
        from nltk.stem import WordNetLemmatizer
        nltk.data.path.insert(0, _NLTK_DATA_DIR)
        nltk.download("wordnet", download_dir=_NLTK_DATA_DIR, quiet=True)
        _lemmatizer = WordNetLemmatizer()
    return [_lemmatizer.lemmatize(t) for t in tokens]


_lemmatize_udf = F.udf(_lemmatize_tokens, ArrayType(StringType()))


@dataclass
class TopicModelResults:
    video_topics: list[dict]
    topic_words: list[dict]


class TopicModelingStage:
    """Runs the Spark ML pipeline: tokenize → remove stop words → lemmatize → vectorize → LDA."""

    def __init__(self, max_iter: int, max_topics: int = 10) -> None:
        self._max_iter = max_iter
        self._max_topics = max_topics

    @staticmethod
    def choose_num_topics(num_docs: int, max_topics: int = 10) -> int:
        """Pick k based on corpus size: k = clamp(num_docs // 3, 2, max_topics)."""
        return max(2, min(num_docs // 3, max_topics))

    def run(self, transcript_df: DataFrame) -> tuple[LDAModel, CountVectorizerModel, DataFrame]:
        """Fit the full ML pipeline and return the trained models and transformed DataFrame."""
        num_docs = transcript_df.count()
        num_topics = self.choose_num_topics(num_docs, self._max_topics)
        min_df = max(1.0, num_docs * 0.05)

        print(f"  [LDA] corpus={num_docs} docs, k={num_topics} topics, minDF={min_df:.1f}")

        tokenizer = RegexTokenizer(
            inputCol="transcript", outputCol="tokens", pattern=r"\W+", minTokenLength=3
        )
        remover = StopWordsRemover(inputCol="tokens", outputCol="filtered")
        vectorizer = CountVectorizer(
            inputCol="lemmatized", outputCol="features", vocabSize=5000, minDF=min_df
        )
        lda = LDA(k=num_topics, maxIter=self._max_iter, seed=42, featuresCol="features")

        tokenized = tokenizer.transform(transcript_df)
        filtered = remover.transform(tokenized)
        lemmatized = filtered.withColumn("lemmatized", _lemmatize_udf(F.col("filtered")))
        cv_model = vectorizer.fit(lemmatized)
        vectorized = cv_model.transform(lemmatized)

        lda_model = lda.fit(vectorized)
        transformed = lda_model.transform(vectorized)
        return lda_model, cv_model, transformed

    def get_topic_words_map(self, lda_model: LDAModel, cv_model: CountVectorizerModel) -> dict[int, list[str]]:
        """Return a mapping of topic_id -> list of top words (for labeling)."""
        vocabulary = cv_model.vocabulary
        result = {}
        for t in lda_model.describeTopics(maxTermsPerTopic=10).collect():
            result[t.topic] = [vocabulary[i] for i in t.termIndices]
        return result

    def extract_results(
        self,
        lda_model: LDAModel,
        transformed_df: DataFrame,
        cv_model: CountVectorizerModel,
        run_id: str,
        run_date,
        topic_labels: dict[int, str] | None = None,
    ) -> TopicModelResults:
        """Extract per-topic word distributions and per-video topic assignments."""
        labels = topic_labels or {}
        vocabulary = cv_model.vocabulary
        topic_words = self._extract_topic_words(lda_model, vocabulary, run_id, run_date, labels)
        video_topics = self._extract_video_topics(lda_model, transformed_df, run_id, run_date, labels)
        return TopicModelResults(video_topics=video_topics, topic_words=topic_words)

    def describe_topics(self, lda_model: LDAModel, cv_model: CountVectorizerModel) -> None:
        """Print the top words for each topic to stdout."""
        vocabulary = cv_model.vocabulary
        for t in lda_model.describeTopics(maxTermsPerTopic=5).collect():
            words = [vocabulary[i] for i in t.termIndices]
            print(f"  Topic {t.topic}: {', '.join(words)}")

    # ── Private extraction helpers ────────────────────────────────────────

    def _extract_topic_words(
        self, lda_model: LDAModel, vocabulary: list[str], run_id: str, run_date, labels: dict[int, str]
    ) -> list[dict]:
        rows = []
        for t in lda_model.describeTopics(maxTermsPerTopic=10).collect():
            for idx, weight in zip(t.termIndices, t.termWeights):
                rows.append({
                    "run_id": run_id,
                    "run_date": run_date,
                    "topic_id": t.topic,
                    "topic_label": labels.get(t.topic, ""),
                    "word": vocabulary[idx],
                    "weight": float(weight),
                })
        return rows

    def _extract_video_topics(
        self, lda_model: LDAModel, transformed_df: DataFrame, run_id: str, run_date, labels: dict[int, str]
    ) -> list[dict]:
        num_topics = lda_model.describeTopics().count()
        rows = []
        for doc in transformed_df.select(
            "video_id", "channel_id", "channel_name", "satisfaction_pct", "topicDistribution"
        ).collect():
            dist = doc.topicDistribution.toArray()
            dominant = int(np.argmax(dist))
            for t in range(num_topics):
                rows.append({
                    "run_id": run_id,
                    "run_date": run_date,
                    "video_id": doc.video_id,
                    "channel_id": doc.channel_id,
                    "channel_name": doc.channel_name or doc.channel_id,
                    "topic_id": t,
                    "topic_label": labels.get(t, ""),
                    "topic_weight": float(dist[t]),
                    "dominant_topic": dominant,
                    "satisfaction_pct": float(doc.satisfaction_pct),
                })
        return rows
