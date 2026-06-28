# Runbooks

Operational playbooks for OSS Pulse. Each runbook is action-first:
the title says what scenario it solves, the headings inside are the
literal steps to take.

| Runbook | Scenario |
|---------|----------|
| [backfill.md](backfill.md) | Re-ingest an arbitrary date range |
| [schema_change.md](schema_change.md) | Upstream payload added/removed/renamed a field |
| [data_missing.md](data_missing.md) | "Yesterday's numbers look light" — root-cause diagnosis |
| [airflow_setup.md](airflow_setup.md) | One-time local Airflow scheduler setup |

Each runbook ends with a **Verification** section — how to know you
fixed the thing, not just shipped a change.
