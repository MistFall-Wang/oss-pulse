"""Replay one hour of PushEvents from Bronze (or a raw GH Archive file)
to a Kafka topic on the local Redpanda broker.

Sprint 6 streaming MVP — minimum demo. The point is to prove the
batch→stream→reconciliation triangle works on real data, not to ship
a production replay system. Sprint 7-9 (optional) extend this to
time-warped replay, watermarks, and continuous reconciliation.

Usage:
    uv run python -m streaming.replay \\
        --source data/raw/2025-01-15-12.json.gz \\
        --topic gh-events \\
        --event-type PushEvent

The script preserves the original `created_at` on the message value
(so the consumer can do event-time processing later) and sets the
Kafka message key to `repo_id` so events for the same repo land on
the same partition.
"""

from __future__ import annotations

import argparse
import gzip
import json
import sys
import time

from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError


def ensure_topic(bootstrap: str, topic: str, partitions: int = 3) -> None:
    admin = KafkaAdminClient(bootstrap_servers=bootstrap, client_id="oss-pulse-replay")
    try:
        admin.create_topics(
            [NewTopic(name=topic, num_partitions=partitions, replication_factor=1)]
        )
        print(f"[replay] created topic {topic!r} with {partitions} partitions")
    except TopicAlreadyExistsError:
        print(f"[replay] topic {topic!r} already exists, reusing")
    admin.close()


def replay(source: str, bootstrap: str, topic: str, event_type: str) -> int:
    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda v: v.encode("utf-8"),
        key_serializer=lambda k: str(k).encode("utf-8") if k is not None else None,
        acks="all",
        linger_ms=10,
    )
    sent = 0
    skipped = 0
    t0 = time.perf_counter()
    with gzip.open(source, "rt") as src:
        for line in src:
            event = json.loads(line)
            if event.get("type") != event_type:
                skipped += 1
                continue
            key = event.get("repo", {}).get("id")
            producer.send(topic, key=key, value=line.rstrip("\n"))
            sent += 1
            if sent % 10_000 == 0:
                print(f"[replay] sent {sent:,} so far ...")
    producer.flush()
    producer.close()
    wall = time.perf_counter() - t0
    print(
        f"[replay] done. sent={sent:,} skipped={skipped:,} "
        f"wall={wall:.1f}s rate={sent / wall:.0f} msg/s"
    )
    return sent


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", required=True, help="path to GH Archive .json.gz")
    parser.add_argument(
        "--bootstrap", default="localhost:19094", help="Redpanda Kafka bootstrap"
    )
    parser.add_argument("--topic", default="gh-events")
    parser.add_argument(
        "--event-type",
        default="PushEvent",
        help="only events of this type are replayed (MVP: one type at a time)",
    )
    args = parser.parse_args()

    ensure_topic(args.bootstrap, args.topic)
    sent = replay(args.source, args.bootstrap, args.topic, args.event_type)
    sys.exit(0 if sent > 0 else 2)


if __name__ == "__main__":
    main()
