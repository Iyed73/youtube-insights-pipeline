from __future__ import annotations

import clickhouse_connect

from utils.config import ClickHouseConfig
from utils.topics import LdaResult

_TOPIC_SUMMARY = "analytics.topic_summary"
_TOPIC_WORDS = "analytics.topic_words"
_SUMMARY_COLUMNS = [
    "run_id", "run_date", "topic_id", "topic_label", "avg_satisfaction", "video_count",
]
_WORDS_COLUMNS = ["run_id", "run_date", "topic_id", "topic_label", "word", "weight"]


class ClickHouseWriter:
    def __init__(self, cfg: ClickHouseConfig) -> None:
        self._client = clickhouse_connect.get_client(
            host=cfg.host, port=cfg.port, username=cfg.user, password=cfg.password
        )

    def publish(self, result: LdaResult, labels: dict[int, str]) -> None:
        summary_rows = [
            [
                result.run_id, result.run_date, topic.topic_id, labels[topic.topic_id],
                topic.avg_satisfaction, topic.video_count,
            ]
            for topic in result.topics
        ]
        word_rows = [
            [
                result.run_id, result.run_date, topic.topic_id, labels[topic.topic_id],
                word.word, word.weight,
            ]
            for topic in result.topics
            for word in topic.words
        ]
        summary_staging = self._stage(_TOPIC_SUMMARY, _SUMMARY_COLUMNS, summary_rows)
        words_staging = self._stage(_TOPIC_WORDS, _WORDS_COLUMNS, word_rows)

        # Swapping in fully written staging tables means dashboards never see a
        # half-written table; afterwards the staging tables hold the previous run.
        self._client.command(f"EXCHANGE TABLES {summary_staging} AND {_TOPIC_SUMMARY}")
        self._client.command(f"EXCHANGE TABLES {words_staging} AND {_TOPIC_WORDS}")
        self._client.command(f"DROP TABLE {summary_staging}")
        self._client.command(f"DROP TABLE {words_staging}")

    def _stage(self, table: str, columns: list[str], rows: list[list]) -> str:
        staging = f"{table}_staging"
        self._client.command(f"DROP TABLE IF EXISTS {staging}")
        self._client.command(f"CREATE TABLE {staging} AS {table}")
        if rows:
            self._client.insert(staging, rows, column_names=columns)
        return staging
