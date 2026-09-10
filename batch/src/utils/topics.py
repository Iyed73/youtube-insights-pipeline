from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime


@dataclass(frozen=True)
class TopicWord:
    word: str
    weight: float


@dataclass(frozen=True)
class Topic:
    topic_id: int
    words: list[TopicWord]  # highest weight first
    video_count: int  # videos whose dominant topic this is
    avg_satisfaction: float  # mean satisfaction_pct of those videos (0 if none)


@dataclass(frozen=True)
class LdaResult:
    run_id: str
    run_date: datetime
    topics: list[Topic]

    def to_json(self) -> bytes:
        payload = {
            "run_id": self.run_id,
            "run_date": self.run_date.isoformat(),
            "topics": [asdict(topic) for topic in self.topics],
        }
        return json.dumps(payload).encode("utf-8")

    @classmethod
    def from_json(cls, data: bytes) -> LdaResult:
        payload = json.loads(data)
        return cls(
            run_id=payload["run_id"],
            run_date=datetime.fromisoformat(payload["run_date"]),
            topics=[
                Topic(
                    topic_id=topic["topic_id"],
                    words=[TopicWord(**word) for word in topic["words"]],
                    video_count=topic["video_count"],
                    avg_satisfaction=topic["avg_satisfaction"],
                )
                for topic in payload["topics"]
            ],
        )
