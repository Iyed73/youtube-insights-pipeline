from __future__ import annotations

import anthropic


def label_topics(api_key: str, topic_words: dict[int, list[str]]) -> dict[int, str]:
    """Send each topic's top words to Claude and return a short label per topic.

    Args:
        api_key: Anthropic API key.
        topic_words: Mapping of topic_id -> list of top words.

    Returns:
        Mapping of topic_id -> short human-readable label.
    """
    if not api_key:
        print("  [label] ANTHROPIC_API_KEY not set — skipping topic labeling")
        return {tid: f"Topic {tid}" for tid in topic_words}

    client = anthropic.Anthropic(api_key=api_key)

    topics_text = "\n".join(
        f"Topic {tid}: {', '.join(words)}" for tid, words in sorted(topic_words.items())
    )

    message = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=256,
        messages=[
            {
                "role": "user",
                "content": (
                    "Below are topics from an LDA model run on YouTube video transcripts. "
                    "Each topic is represented by its top keywords.\n\n"
                    f"{topics_text}\n\n"
                    "For each topic, reply with ONLY a short label (2-5 words) that captures "
                    "the theme. Format: one line per topic, exactly like:\n"
                    "0: Label Here\n"
                    "1: Another Label\n"
                    "No extra text."
                ),
            }
        ],
    )

    labels = {}
    for line in message.content[0].text.strip().splitlines():
        line = line.strip()
        if not line or ":" not in line:
            continue
        tid_str, label = line.split(":", 1)
        try:
            tid = int(tid_str.strip())
            labels[tid] = label.strip()
        except ValueError:
            continue

    # Fill in any missing topics
    for tid in topic_words:
        if tid not in labels:
            labels[tid] = f"Topic {tid}"

    return labels
