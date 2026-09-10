from __future__ import annotations

from utils.config import MinioConfig
from utils.storage import ObjectStore
from utils.topics import LdaResult


class LdaResultStore:
    def __init__(self, cfg: MinioConfig) -> None:
        self._store = ObjectStore(cfg)

    def save(self, result: LdaResult) -> None:
        key = _key(result.run_id)
        payload = result.to_json()
        self._store.write(key, payload, content_type="application/json")
        print(f"  [lda_store] saved {len(payload):,} bytes → {key}")

    def load(self, run_id: str) -> LdaResult:
        key = _key(run_id)
        payload = self._store.read(key)
        if payload is None:
            raise FileNotFoundError(f"No LDA result for run {run_id!r} at {key}")
        return LdaResult.from_json(payload)


def _key(run_id: str) -> str:
    return f"batch-runs/{run_id}/lda_result.json"
