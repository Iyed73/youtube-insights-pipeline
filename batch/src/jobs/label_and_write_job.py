"""
Label and Write Job
===================
Step 3 of the batch pipeline (DAG task: label_and_write).

Reads the LDA output JSON written to MinIO by lda_job, calls the Claude
API to generate a short human-readable label for each topic, patches those
labels into the pre-extracted result rows, then writes everything to
ClickHouse.

No Spark session required — this job is pure Python (Claude API + ClickHouse).
"""

from __future__ import annotations

import time
from datetime import datetime

from utils.clickhouse_writer import ClickHouseWriter
from utils.config import BatchConfig
from utils.lda_store import LdaStore
from utils.topic_labeler import label_topics
from utils.transcribe import MinioCfg


class LabelAndWriteJob:
    def __init__(self, cfg: BatchConfig) -> None:
        self._cfg = cfg

    def run(self) -> None:
        print("=== Label and Write Job ===")

        minio_cfg = MinioCfg(
            endpoint=self._cfg.minio_endpoint,
            access_key=self._cfg.minio_access_key,
            secret_key=self._cfg.minio_secret_key,
            bucket=self._cfg.minio_bucket,
        )

        # ── Step 1: Load LDA output from MinIO ────────────────────────────
        print("\n[Step 1/3] Loading LDA output from MinIO...")
        store = LdaStore(minio_cfg)
        data = store.load()

        run_id: str = data["run_id"]
        # run_date was serialised to ISO string — restore to datetime for ClickHouse.
        run_date: datetime = datetime.fromisoformat(data["run_date"])
        topic_words_map: dict[int, list[str]] = data["topic_words_map"]
        video_topics: list[dict] = data["video_topics"]
        topic_words: list[dict] = data["topic_words"]

        print(f"  run_id:   {run_id}")
        print(f"  run_date: {run_date}")
        print(f"  topics:   {len(topic_words_map)}")
        print(f"  videos:   {len({r['video_id'] for r in video_topics})}")

        # ── Step 2: Label topics via Claude ───────────────────────────────
        print("\n[Step 2/3] Labeling topics via Claude API...")
        t0 = time.time()
        labels = label_topics(self._cfg.anthropic_api_key, topic_words_map)
        for tid, label in sorted(labels.items()):
            print(f"  Topic {tid}: {label}")
        print(f"  Done in {time.time() - t0:.1f}s.")

        # Patch labels and restore run_date (was serialised to string by JSON).
        for row in video_topics:
            row["topic_label"] = labels.get(row["topic_id"], "")
            row["run_date"] = run_date
        for row in topic_words:
            row["topic_label"] = labels.get(row["topic_id"], "")
            row["run_date"] = run_date

        # ── Step 3: Write to ClickHouse ───────────────────────────────────
        print("\n[Step 3/3] Writing results to ClickHouse...")
        t0 = time.time()
        writer = ClickHouseWriter(self._cfg)
        writer.ensure_tables()
        writer.delete_previous_runs()
        writer.write_video_topics(video_topics)
        writer.write_topic_words(topic_words)
        print(
            f"  Wrote {len(video_topics)} video_topics and {len(topic_words)} topic_words "
            f"in {time.time() - t0:.1f}s."
        )

        print("\n=== Label and write complete ===")


def main() -> None:
    LabelAndWriteJob(BatchConfig()).run()


if __name__ == "__main__":
    main()
