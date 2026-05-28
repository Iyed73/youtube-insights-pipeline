"""
Isolated UDF implementations for Phase 2 topic modeling.

MUST stay import-minimal — cloudpickle serialises this module's globals
when shipping UDFs to executors.  Heavy MLlib / pipeline imports must
never appear here.
"""
from __future__ import annotations
import json


def extract_dominant(vector):
    """Return the index of the highest-probability topic."""
    if vector is None:
        return None
    vals = vector.toArray().tolist()
    return int(vals.index(max(vals)))


def serialize_distribution(vector):
    """Serialise a topic-distribution vector to a JSON string."""
    if vector is None:
        return None
    return json.dumps([
        {"topic_id": i, "probability": round(p, 6)}
        for i, p in enumerate(vector.toArray().tolist())
    ])