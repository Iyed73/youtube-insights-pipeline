from __future__ import annotations

import clickhouse_connect
from clickhouse_connect.driver.client import Client

from utils.config import BatchConfig


class ClickHouseWriter:
    """Manages ClickHouse table creation and batch inserts for pipeline results."""

    def __init__(self, cfg: BatchConfig) -> None:
        self._client: Client = clickhouse_connect.get_client(
            host=cfg.clickhouse_host,
            port=cfg.clickhouse_http_port,
            username=cfg.clickhouse_user,
            password=cfg.clickhouse_password,
        )

    def ensure_tables(self) -> None:
        """Create batch tables if they don't already exist."""
        self._client.command("""
            CREATE TABLE IF NOT EXISTS analytics.topic_summary (
                run_id           String,
                run_date         DateTime,
                topic_id         UInt8,
                topic_label      String DEFAULT '',
                avg_satisfaction Float32,
                video_count      UInt32
            ) ENGINE = MergeTree()
            ORDER BY (topic_id)
        """)
        self._client.command("""
            CREATE TABLE IF NOT EXISTS analytics.topic_words (
                run_id      String,
                run_date    DateTime,
                topic_id    UInt8,
                topic_label String DEFAULT '',
                word        String,
                weight      Float32
            ) ENGINE = MergeTree()
            ORDER BY (topic_id, weight)
        """)

    def delete_previous_runs(self) -> None:
        """Delete all existing batch results so the new run fully replaces them."""
        self._client.command("TRUNCATE TABLE analytics.topic_summary")
        self._client.command("TRUNCATE TABLE analytics.topic_words")

    def write_topic_summary(self, rows: list[dict]) -> None:
        if not rows:
            return
        columns = ["run_id", "run_date", "topic_id", "topic_label", "avg_satisfaction", "video_count"]
        self._client.insert(
            "analytics.topic_summary",
            [[r[c] for c in columns] for r in rows],
            column_names=columns,
        )

    def write_topic_words(self, rows: list[dict]) -> None:
        if not rows:
            return
        columns = ["run_id", "run_date", "topic_id", "topic_label", "word", "weight"]
        self._client.insert(
            "analytics.topic_words",
            [[r[c] for c in columns] for r in rows],
            column_names=columns,
        )
