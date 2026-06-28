# Streaming MVP (Sprint 6)

Minimum end-to-end demo of GH Archive replay → Kafka → Structured
Streaming → parallel Silver Delta table → batch-vs-streaming
reconciliation.

**Scope** (deliberately narrow, see PROJECT_PLAN.md):
- One hour of one event type (PushEvent) replayed to a single Kafka
  topic on a single Redpanda broker
- Single Spark Structured Streaming consumer using `availableNow`
  trigger (one-shot drain), foreachBatch + Delta MERGE for idempotency
- Reconciliation script compares to the batch-built Silver table for
  the same `ingest_hour`

Sprint 7-9 (optional) extends this to time-warped replay,
watermark-based late-event handling, and continuous reconciliation.

## Bring up

```bash
docker-compose -f streaming/docker-compose.yml up -d
sleep 5
docker exec oss-pulse-redpanda rpk cluster health   # expect Healthy: true
```

## Demo run

```bash
export JAVA_HOME=/Library/Java/JavaVirtualMachines/amazon-corretto-17.jdk/Contents/Home
export PATH=$JAVA_HOME/bin:$PATH

# 1. Replay 2025-01-15-12 PushEvents to Kafka (~43 s on the local laptop)
uv run python -m streaming.replay --source data/raw/2025-01-15-12.json.gz

# 2. Drain the topic into the streaming silver table (one-shot)
uv run python -m streaming.consumer

# 3. Reconcile against the batch silver table for the same hour
uv run python -m streaming.reconcile --ingest-hour 2025-01-15-12
```

## Demo result (2026-06-28)

```
========== batch ↔ streaming reconciliation ==========
ingest_hour:       2025-01-15-12
batch rows:        181,221
streaming rows:    181,221
row count delta:   +0 (0.0000%)
batch commits Σ:   576,167
streaming commits: 576,167
commits delta:     +0
ids only in batch:    0
ids only in streaming:0

[reconcile] pct < 0.01% AND no orphan ids: True
```

**Zero divergence on 181,221 events.** The reconciliation threshold
in `reconcile.py` is 0.01 %; actual delta is 0.0000 %.

## Why each piece is what it is

| File | Why |
|------|-----|
| `docker-compose.yml` (Redpanda v24.2.7) | Kafka-API compatible, no JVM, no Zookeeper, boots in 2s vs ~30s for Kafka. Pick non-overlapping host ports (19094) so it coexists with other Docker projects. |
| `replay.py` (kafka-python) | Pure-Python, no Spark dependency on the producer side — keeps replay testable independent of the consumer. Key by `repo_id` so events for the same repo land on the same partition. |
| `consumer.py` (Structured Streaming) | foreachBatch + Delta MERGE on `id` gives exactly-once writes by virtue of MERGE idempotency — no separate offset tracking needed. availableNow trigger drains and exits, fitting batch-style ops. |
| `reconcile.py` | Two cheap aggregates + a set-difference on `id`. The set-difference catches the case where row counts happen to match but the ids differ (i.e. streaming dropped some events and gained different ones — what you'd see if a producer failure mid-replay re-fired with a different starting offset). |

## Tear down

```bash
docker-compose -f streaming/docker-compose.yml down -v
rm -rf data/streaming/
```

## What this does NOT cover

- **Continuous replay / event-time semantics** — Sprint 7
- **Watermark-based late-event handling** — Sprint 8
- **Cost / latency SLO under sustained throughput** — Sprint 9
- **Schema-registry coordination across producer + consumer** — out of
  scope for portfolio MVP
- **Replication factor > 1 / multi-broker fault tolerance** — single
  broker, single partition replication. ADR-0008 (Sprint 7) will
  codify the production cluster sizing.

## Senior-signal payoff

This 4-file MVP satisfies the "batch + streaming story" entry on
PROJECT_PLAN.md's seven senior signals:

> 6. **Batch + streaming story** — GH Archive replay to Kafka,
>    Structured Streaming consumer, reconciliation against batch.

…and provides a defensible interview answer to *"have you actually
reconciled a batch table against a streaming one to a < 0.01 %
threshold"* — yes, on real data, here's the script.
