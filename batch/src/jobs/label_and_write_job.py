from __future__ import annotations

import argparse
import sys
import time

from utils.clickhouse_writer import ClickHouseWriter
from utils.config import ClickHouseConfig, LabelingConfig, MinioConfig
from utils.lda_store import LdaResultStore
from utils.topic_labeler import label_topics


class LabelAndWriteJob:
    def __init__(self, run_id: str) -> None:
        self._run_id = run_id
        self._minio_cfg = MinioConfig.from_env()
        self._clickhouse_cfg = ClickHouseConfig.from_env()
        self._labeling_cfg = LabelingConfig.from_env()

    def run(self) -> int:
        print("=== Label and Write Job ===")

        print("\n[Step 1/3] Loading LDA result from MinIO...")
        result = LdaResultStore(self._minio_cfg).load(self._run_id)
        print(f"  run_id:   {result.run_id}")
        print(f"  run_date: {result.run_date}")
        print(f"  topics:   {len(result.topics)}")

        print(f"\n[Step 2/3] Labeling topics via Claude ({self._labeling_cfg.model})...")
        t0 = time.time()
        labels = label_topics(self._labeling_cfg, result.topics)
        for topic_id, label in sorted(labels.items()):
            print(f"  Topic {topic_id}: {label}")
        print(f"  Done in {time.time() - t0:.1f}s.")

        print("\n[Step 3/3] Writing results to ClickHouse...")
        t0 = time.time()
        ClickHouseWriter(self._clickhouse_cfg).publish(result, labels)
        print(f"  Published {len(result.topics)} topic(s) in {time.time() - t0:.1f}s.")

        print("\n=== Label and write complete ===")
        return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Label LDA topics and publish them.")
    parser.add_argument(
        "--run-id", required=True, help="Pipeline run id of the LDA result to publish."
    )
    return LabelAndWriteJob(parser.parse_args().run_id).run()


if __name__ == "__main__":
    sys.exit(main())
