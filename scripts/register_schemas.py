#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import requests

_SCHEMAS_DIR = Path(__file__).parent.parent / "schemas"

TOPIC_SCHEMAS: dict[str, str] = {
    "channel-discovery": "channel-discovery.avsc",
    "raw-comments": "raw-comments.avsc",
}


def register_schema(registry_url: str, subject: str, schema_str: str) -> int:
    url = f"{registry_url}/subjects/{subject}/versions"
    resp = requests.post(
        url,
        data=json.dumps({"schema": schema_str}),
        headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
        timeout=10,
    )
    try:
        resp.raise_for_status()
    except requests.HTTPError as exc:
        print(f"  ✗  {subject}: HTTP {resp.status_code} — {resp.text}", file=sys.stderr)
        raise SystemExit(1) from exc

    schema_id: int = resp.json()["id"]
    print(f"  ✓  {subject}  (schema id={schema_id})")
    return schema_id


def main() -> None:
    registry_url = os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081").rstrip("/")
    print(f"Schema Registry: {registry_url}\n")

    for topic, schema_file in TOPIC_SCHEMAS.items():
        schema_path = _SCHEMAS_DIR / schema_file
        if not schema_path.exists():
            print(
                f"  ✗  {topic}: schema file not found at {schema_path}",
                file=sys.stderr,
            )
            raise SystemExit(1)

        subject = f"{topic}-value"
        register_schema(registry_url, subject, schema_path.read_text())

    print("\nAll schemas registered successfully.")


if __name__ == "__main__":
    main()
