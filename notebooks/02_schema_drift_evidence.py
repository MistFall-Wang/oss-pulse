"""
Sprint 0 sidequest: prove schema drift across years.

Compares GH Archive event and payload schemas between 2015, 2018, and 2025
samples. The output is raw evidence for ADR-001.
"""

from __future__ import annotations

import gzip
import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

SAMPLES = {
    "2015-01-15-12": Path("data/raw/2015-01-15-12.json.gz"),
    "2018-01-15-12": Path("data/raw/2018-01-15-12.json.gz"),
    "2025-01-15-12": Path("data/raw/2025-01-15-12.json.gz"),
}
FOCUS_TYPES = ("PushEvent", "IssueCommentEvent")
MAX_PATH_DEPTH = 4


def load(path: Path) -> list[dict[str, Any]]:
    events = []
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        for line in fh:
            events.append(json.loads(line))
    return events


def type_name(value: Any) -> str:
    if isinstance(value, dict):
        return "object"
    if isinstance(value, list):
        return "array"
    return type(value).__name__


def collect_paths(value: Any, prefix: str = "", depth: int = 0) -> set[str]:
    if depth >= MAX_PATH_DEPTH:
        return {prefix} if prefix else set()
    if isinstance(value, dict):
        paths = set()
        for key, child in value.items():
            child_prefix = f"{prefix}.{key}" if prefix else key
            paths.add(child_prefix)
            paths.update(collect_paths(child, child_prefix, depth + 1))
        return paths
    if isinstance(value, list):
        paths = {prefix} if prefix else set()
        for child in value[:3]:
            paths.update(collect_paths(child, f"{prefix}[]", depth + 1))
        return paths
    return {prefix} if prefix else set()


def print_sorted_list(values: set[str], indent: str = "    ") -> None:
    print(f"{indent}{sorted(values)}")


missing = [str(path) for path in SAMPLES.values() if not path.exists()]
if missing:
    raise FileNotFoundError(f"missing sample files: {missing}")

samples = {label: load(path) for label, path in SAMPLES.items()}

print(f"\n{'=' * 70}")
print("SCHEMA DRIFT EVIDENCE")
print(f"{'=' * 70}\n")


print("A. Event counts and event type sets across years")
print("-" * 70)
for label, events in samples.items():
    type_counts = Counter(event["type"] for event in events)
    print(f"  {label}: {len(events):,} events, {len(type_counts):2d} distinct types")
    print(f"    types = {sorted(type_counts)}")
    print("    top 5 =")
    for event_type, count in type_counts.most_common(5):
        print(f"      {event_type:30s} {count:>8,}")
    print()


print("\nB. Top-level field set across years")
print("-" * 70)
top_fields_by_year = {}
for label, events in samples.items():
    top_fields = set()
    for event in events:
        top_fields.update(event)
    top_fields_by_year[label] = top_fields
    print(f"  {label}: {sorted(top_fields)}")

oldest_label, newest_label = min(top_fields_by_year), max(top_fields_by_year)
print(f"\n  [{oldest_label} -> {newest_label}]")
print_sorted_list(top_fields_by_year[newest_label] - top_fields_by_year[oldest_label], "    ADDED:   ")
print_sorted_list(top_fields_by_year[oldest_label] - top_fields_by_year[newest_label], "    REMOVED: ")


print("\n\nC. Top-level value types across years")
print("-" * 70)
top_type_observations: dict[str, defaultdict[str, set[str]]] = {}
for label, events in samples.items():
    field_types: defaultdict[str, set[str]] = defaultdict(set)
    for event in events:
        for field, value in event.items():
            field_types[field].add(type_name(value))
    top_type_observations[label] = field_types
    print(f"  {label}")
    for field in sorted(field_types):
        print(f"    {field:12s}: {sorted(field_types[field])}")


print("\n\nD. Nested entity id types across years")
print("-" * 70)
for label, events in samples.items():
    print(f"  {label}")
    for entity in ("actor", "repo", "org"):
        present = 0
        id_types = set()
        for event in events:
            obj = event.get(entity)
            if not obj:
                continue
            present += 1
            id_types.add(type_name(obj.get("id")))
        print(f"    {entity:5s}: present={present:>7,}/{len(events):,} id_types={sorted(id_types)}")


print("\n\nE. payload field schema drift by event type")
print("-" * 70)
for event_type in FOCUS_TYPES:
    print(f"\n  >>> {event_type} <<<")
    payload_keys_by_year: dict[str, set[str]] = {}
    payload_paths_by_year: dict[str, set[str]] = {}

    for label, events in samples.items():
        keys = set()
        paths = set()
        count = 0
        for event in events:
            if event["type"] != event_type:
                continue
            count += 1
            payload = event.get("payload") or {}
            keys.update(payload)
            paths.update(collect_paths(payload))
        payload_keys_by_year[label] = keys
        payload_paths_by_year[label] = paths
        print(f"    {label} ({count:>6,} events)")
        print(f"      first-level keys = {sorted(keys)}")
        print(f"      nested path count <= depth {MAX_PATH_DEPTH}: {len(paths):,}")

    labels = sorted(payload_keys_by_year)
    oldest, newest = labels[0], labels[-1]
    added_keys = payload_keys_by_year[newest] - payload_keys_by_year[oldest]
    removed_keys = payload_keys_by_year[oldest] - payload_keys_by_year[newest]
    added_paths = payload_paths_by_year[newest] - payload_paths_by_year[oldest]
    removed_paths = payload_paths_by_year[oldest] - payload_paths_by_year[newest]

    print(f"\n    [{oldest} -> {newest}] first-level payload keys")
    print(f"      ADDED fields:   {sorted(added_keys) if added_keys else '(none)'}")
    print(f"      REMOVED fields: {sorted(removed_keys) if removed_keys else '(none)'}")
    print(f"    [{oldest} -> {newest}] nested payload paths <= depth {MAX_PATH_DEPTH}")
    print(f"      ADDED path count:   {len(added_paths):,}")
    print(f"      REMOVED path count: {len(removed_paths):,}")
    print(f"      ADDED path sample:   {sorted(added_paths)[:25] if added_paths else '(none)'}")
    print(f"      REMOVED path sample: {sorted(removed_paths)[:25] if removed_paths else '(none)'}")


print("\n\nF. Sample 2015 PushEvent structure")
print("-" * 70)
push_2015 = next(
    (event for event in samples["2015-01-15-12"] if event["type"] == "PushEvent"),
    None,
)
if push_2015:
    print(json.dumps(push_2015, indent=2, ensure_ascii=False)[:2000])
else:
    print("  No PushEvent found in the 2015 sample.")


print(f"\n{'=' * 70}")
print("Done. Schema drift is now empirical, not assumed.")
print(f"{'=' * 70}\n")
