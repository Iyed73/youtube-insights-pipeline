from __future__ import annotations

from collections import defaultdict
from statistics import fmean

from nltk.stem import WordNetLemmatizer
from pyspark.ml.clustering import LDA, LDAModel
from pyspark.ml.feature import CountVectorizer, RegexTokenizer, StopWordsRemover
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType

from utils.config import LdaConfig
from utils.topics import Topic, TopicWord

MIN_DOCUMENTS = 6
MIN_TOPICS = 3
WORDS_PER_TOPIC = 20

# The wordnet corpus is baked into the Spark image (NLTK_DATA).
_lemmatizer = WordNetLemmatizer()


def _lemmatize_tokens(tokens: list[str]) -> list[str]:
    return [_lemmatizer.lemmatize(token) for token in tokens]


_lemmatize_udf = F.udf(_lemmatize_tokens, ArrayType(StringType()))


class TopicModelingStage:
    def __init__(self, cfg: LdaConfig) -> None:
        self._cfg = cfg

    @staticmethod
    def choose_num_topics(num_docs: int, max_topics: int) -> int:
        return min(max(num_docs // 3, MIN_TOPICS), max_topics)

    def run(self, transcripts: DataFrame) -> list[Topic]:
        num_docs = transcripts.count()
        num_topics = self.choose_num_topics(num_docs, self._cfg.max_topics)
        # Filters single-video jargon that produces noise topics.
        min_df = max(2.0, num_docs * 0.10)
        vocab_size = min(2000, num_docs * 100)
        print(
            f"  [LDA] corpus={num_docs} docs, k={num_topics} topics, "
            f"minDF={min_df:.1f}, vocabSize={vocab_size}"
        )

        tokens = RegexTokenizer(
            inputCol="transcript", outputCol="tokens", pattern=r"\W+", minTokenLength=3
        ).transform(transcripts)
        filtered = StopWordsRemover(inputCol="tokens", outputCol="filtered").transform(tokens)
        lemmatized = filtered.withColumn("lemmatized", _lemmatize_udf("filtered"))
        vectorizer = CountVectorizer(
            inputCol="lemmatized",
            outputCol="features",
            vocabSize=vocab_size,
            minDF=min_df,
            maxDF=0.95,
        ).fit(lemmatized)
        # LDA passes over the data many times; don't rerun the lemmatize UDF each time.
        features = vectorizer.transform(lemmatized).cache()

        lda_model = LDA(k=num_topics, maxIter=self._cfg.max_iter, seed=42).fit(features)
        print(
            f"  [LDA] log-likelihood: {lda_model.logLikelihood(features):.2f}  "
            f"log-perplexity: {lda_model.logPerplexity(features):.4f}"
        )

        words = self._top_words(lda_model, vectorizer.vocabulary)
        satisfactions = self._satisfaction_by_dominant_topic(lda_model.transform(features))
        return [
            Topic(
                topic_id=topic_id,
                words=words[topic_id],
                video_count=len(satisfactions[topic_id]),
                avg_satisfaction=fmean(satisfactions[topic_id]) if satisfactions[topic_id] else 0.0,
            )
            for topic_id in range(num_topics)
        ]

    @staticmethod
    def _top_words(lda_model: LDAModel, vocabulary: list[str]) -> dict[int, list[TopicWord]]:
        return {
            row.topic: [
                TopicWord(word=vocabulary[index], weight=float(weight))
                for index, weight in zip(row.termIndices, row.termWeights, strict=True)
            ]
            for row in lda_model.describeTopics(maxTermsPerTopic=WORDS_PER_TOPIC).collect()
        }

    @staticmethod
    def _satisfaction_by_dominant_topic(transformed: DataFrame) -> dict[int, list[float]]:
        by_topic: dict[int, list[float]] = defaultdict(list)
        for row in transformed.select("satisfaction_pct", "topicDistribution").collect():
            by_topic[int(row.topicDistribution.toArray().argmax())].append(row.satisfaction_pct)
        return by_topic
