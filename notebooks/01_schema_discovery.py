"""
Sprint 0 - Step 1: Schema Discovery.

Goal: answer 7 design questions that determine the Bronze schema.
Output: docs/schema_discovery.md is written from this script's output.
"""

from __future__ import annotations

import gzip
import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

RAW_DIR = Path("data/raw")
FILES = sorted(RAW_DIR.glob("2025-01-15-*.json.gz"))
assert len(FILES) >= 2, "expected at least 2 hourly files"


def load_events(files: list[Path]) -> list[dict[str, Any]]:
    events = []
    for path in files:
        with gzip.open(path, "rt", encoding="utf-8") as fh:
            for line in fh:
                events.append(json.loads(line))
    return events


events = load_events(FILES)
print(f"\n{'=' * 60}")
print(f"Loaded {len(events):,} events from {len(FILES)} hourly files")
print(f"{'=' * 60}\n")


# Q1: Event type distribution.
print("Q1. Event type distribution")
print("-" * 60)
type_counts = Counter(event["type"] for event in events)
total = sum(type_counts.values())
for event_type, count in type_counts.most_common():
    print(f"  {event_type:30s} {count:>8,}  ({count / total * 100:5.2f}%)")
print(f"  TOTAL distinct types: {len(type_counts)}")


# Q2: Top-level field stability.
print("\n\nQ2. Top-level field presence rate")
print("-" * 60)
top_field_counts: Counter[str] = Counter()
for event in events:
    for key in event:
        top_field_counts[key] += 1
for key, count in top_field_counts.most_common():
    pct = count / len(events) * 100
    flag = "  universal" if pct == 100 else "  partial"
    print(f"  {key:15s} {count:>7,}/{len(events):,}  ({pct:6.2f}%){flag}")


# Q3: event_id uniqueness, including the hourly boundary.
print("\n\nQ3. event_id uniqueness")
print("-" * 60)
ids = [event["id"] for event in events]
unique_ids = set(ids)
print(f"  Total events: {len(ids):,}")
print(f"  Unique ids:   {len(unique_ids):,}")
print(f"  Duplicates:   {len(ids) - len(unique_ids):,}")

id_to_hours: defaultdict[str, set[str]] = defaultdict(set)
for path in FILES:
    hour = path.stem.replace(".json", "")
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        for line in fh:
            event = json.loads(line)
            id_to_hours[event["id"]].add(hour)
cross_hour = {
    event_id: hours for event_id, hours in id_to_hours.items() if len(hours) > 1
}
print(f"  IDs appearing in >1 hourly file: {len(cross_hour):,}")
if cross_hour:
    sample = list(cross_hour.items())[:3]
    for event_id, hours in sample:
        print(f"    example: id={event_id} appears in {hours}")


# Q4: Payload schema divergence by event type.
print("\n\nQ4. payload field divergence by event type")
print("-" * 60)
payload_keys_by_type: defaultdict[str, Counter[str]] = defaultdict(Counter)
type_event_count: Counter[str] = Counter()
for event in events:
    event_type = event["type"]
    type_event_count[event_type] += 1
    payload = event.get("payload") or {}
    for key in payload:
        payload_keys_by_type[event_type][key] += 1

for event_type, _ in type_counts.most_common(5):
    event_count = type_event_count[event_type]
    print(f"\n  [{event_type}] ({event_count:,} events)")
    for key, count in payload_keys_by_type[event_type].most_common(10):
        pct = count / event_count * 100
        print(f"    payload.{key:25s} {count:>7,}/{event_count:,}  ({pct:6.2f}%)")


# Q5: Payload nesting depth.
print("\n\nQ5. payload nesting depth")
print("-" * 60)


def max_depth(obj: Any, current: int = 0) -> int:
    if isinstance(obj, dict):
        if not obj:
            return current
        return max(max_depth(value, current + 1) for value in obj.values())
    if isinstance(obj, list):
        if not obj:
            return current
        return max(max_depth(value, current + 1) for value in obj)
    return current


depths_by_type: defaultdict[str, list[int]] = defaultdict(list)
for event in events:
    depths_by_type[event["type"]].append(max_depth(event.get("payload") or {}))

for event_type, _ in type_counts.most_common(5):
    depths = depths_by_type[event_type]
    print(
        f"  {event_type:25s} max depth={max(depths)}  "
        f"avg={sum(depths) / len(depths):.2f}"
    )


# Q6: Timestamp format and timezone.
print("\n\nQ6. created_at samples")
print("-" * 60)
sample_times = set()
for event in events[:1000]:
    sample_times.add(event.get("created_at"))
    if len(sample_times) >= 5:
        break
for timestamp in sample_times:
    print(f"  {timestamp!r}")


# Q7: actor / repo / org id types.
print("\n\nQ7. actor / repo / org id types")
print("-" * 60)
sample = events[0]
for entity in ("actor", "repo", "org"):
    obj = sample.get(entity)
    if obj:
        print(f"  {entity}: {obj}  (id type = {type(obj.get('id')).__name__})")
    else:
        print(f"  {entity}: not present in sample[0]")

actor_present = sum(1 for event in events if event.get("actor"))
repo_present = sum(1 for event in events if event.get("repo"))
org_present = sum(1 for event in events if event.get("org"))
print(f"\n  actor present in {actor_present:,}/{len(events):,} events")
print(f"  repo  present in {repo_present:,}/{len(events):,} events")
print(f"  org   present in {org_present:,}/{len(events):,} events  (sparse on purpose)")

print(f"\n{'=' * 60}")
print("Done. Now fill in docs/schema_discovery.md based on the above.")
print(f"{'=' * 60}\n")
