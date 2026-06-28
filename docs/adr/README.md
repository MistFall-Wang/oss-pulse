# Architecture Decision Records

This project records every non-trivial architecture decision as an ADR
(format: MADR-lite). A new ADR is required when the decision:

- Locks in a structural choice that is expensive to reverse
- Trades off two reasonable alternatives
- Affects how downstream layers are built

## Index

| # | Title | Status |
| --- | --- | --- |
| 0001 | [Bronze payload handling](0001-payload-handling.md) | Accepted |
| 0002 | event_id as the sole idempotency key | Planned |
| 0003 | Partition Bronze by ingest_hour, ZORDER by created_at | Planned |
| 0004 | No surrogate keys; use GitHub source ids directly | Planned |
| 0005 | Silver schema strategy: tiered, demand-driven | Planned |
| 0006 | Bot identification rules | Planned |
