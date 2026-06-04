from __future__ import annotations

import anthropic
from pydantic import BaseModel

from utils.config import LabelingConfig
from utils.topics import Topic


class _TopicLabel(BaseModel):
    topic_id: int
    label: str


class _TopicLabels(BaseModel):
    labels: list[_TopicLabel]


def label_topics(cfg: LabelingConfig, topics: list[Topic]) -> dict[int, str]:
    labels = {topic.topic_id: f"Topic {topic.topic_id}" for topic in topics}
    if not cfg.api_key:
        print("  [label] ANTHROPIC_API_KEY not set — using generic topic labels")
        return labels

    topics_text = "\n".join(
        f"Topic {topic.topic_id}: {', '.join(word.word for word in topic.words)}"
        for topic in topics
    )
    response = anthropic.Anthropic(api_key=cfg.api_key).messages.parse(
        model=cfg.model,
        max_tokens=4096,
        messages=[
            {
                "role": "user",
                "content": (
                    "Below are topics from an LDA model run on YouTube video transcripts. "
                    "Each topic is represented by its top keywords.\n\n"
                    f"{topics_text}\n\n"
                    "Give each topic a short label (2-5 words) that captures its theme."
                ),
            }
        ],
        output_format=_TopicLabels,
    )
    if response.stop_reason != "end_turn" or response.parsed_output is None:
        raise RuntimeError(
            f"Topic labeling did not complete (stop_reason={response.stop_reason!r})"
        )

    labeled = {
        item.topic_id: item.label
        for item in response.parsed_output.labels
        if item.topic_id in labels
    }
    missing = sorted(labels.keys() - labeled.keys())
    if missing:
        print(f"  [label] no label returned for topic(s) {missing} — using generic labels")
    return {**labels, **labeled}
