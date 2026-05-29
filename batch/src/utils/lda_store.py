"""
MinIO-backed handoff store for intermediate LDA pipeline results.

lda_job writes here after Spark finishes.
label_and_write_job reads from here — no Spark session required.

Object path: {bucket}/batch-runs/latest/lda_output.json
             (always overwritten; only the latest run is kept)
"""

from __future__ import annotations

import io
import json

from minio import Minio

from utils.transcribe import MinioCfg

_KEY = "batch-runs/latest/lda_output.json"


class LdaStore:
    def __init__(self, minio_cfg: MinioCfg) -> None:
        self._cfg = minio_cfg

    def save(self, data: dict) -> None:
        """Serialise *data* and upload it to MinIO.

        ``datetime`` objects are serialised to ISO strings via ``default=str``.
        ``topic_words_map`` keys must be ints — they are stored as strings in
        JSON and restored as ints on :meth:`load`.
        """
        payload = json.dumps(data, default=str).encode("utf-8")
        client = self._make_client()
        client.put_object(
            self._cfg.bucket,
            _KEY,
            io.BytesIO(payload),
            length=len(payload),
            content_type="application/json",
        )
        print(f"  [lda_store] saved {len(payload):,} bytes → {self._cfg.bucket}/{_KEY}")

    def load(self) -> dict:
        """Download and deserialise the LDA output from MinIO.

        Restores ``topic_words_map`` keys to ``int`` (JSON forces string keys).
        """
        client = self._make_client()
        resp = client.get_object(self._cfg.bucket, _KEY)
        data = json.loads(resp.read().decode("utf-8"))
        resp.close()
        resp.release_conn()
        # JSON keys are always strings — restore topic ids to int.
        if "topic_words_map" in data:
            data["topic_words_map"] = {
                int(k): v for k, v in data["topic_words_map"].items()
            }
        return data

    def _make_client(self) -> Minio:
        cfg = self._cfg
        return Minio(cfg.endpoint, access_key=cfg.access_key, secret_key=cfg.secret_key, secure=False)
