# Postmortem 0001 — Silent NULL on PushEvent commit count after upstream rename

- **Date of incident**: 2026-06-28 (deliberately injected in Sprint 5b)
- **Severity**: would have been P2 in prod (poisoned Gold metrics, no
  pipeline failure)
- **Authors**: Peter Wang
- **Incident scripts**:
  [`spark/jobs/incident_inject.py`](../../spark/jobs/incident_inject.py)
- **Fix commit**: this PR

## Summary

A simulated upstream change in GH Archive renamed `payload.size` to
`payload.commit_count` on PushEvent rows. The pipeline did not crash.
Bronze, Silver build, and the silver quality gate all reported
success. The `dbt test` step at the *end* of the pipeline caught the
problem with the precise count: 200 violating rows for
`not_null_events_push_commit_size`.

By the time `dbt test` fired, the Silver build had already completed
and — if the DAG had been run end-to-end — the Gold marts would also
have been built with the poisoned `commit_size` column silently
zero-replaced by `coalesce(sum(...), 0)` in
`repo_daily_activity.total_commits`.

The fix is a one-line `coalesce` in `events_push.sql` to accept both
field names. The systemic fix is moving `commit_size not_null` from
"end-of-pipeline dbt test" to "silver-gate", where it now lives as a
quality.runner check.

## Detection timeline

| Step | Status | Notes |
|------|--------|-------|
| `bronze_ingest` on the synthetic file | PASS | Bronze stores payload as raw JSON STRING per ADR-0001 — by design, no payload-shape validation here |
| `gate_bronze` (4 checks) | PASS | id uniqueness, type-in-set, public, created_at not_null all still hold; payload contents not checked at Bronze |
| `dbt run --select silver.events_push` | PASS | `get_json_object(payload_raw, '$.size')` returned NULL for the 200 incident rows; cast to int returns NULL; the SELECT projects NULL successfully |
| `gate_silver` (7 checks at incident time) | PASS | row-count-match passed because the rows existed (NULLs count as rows); pr_state check covers a different model; **no check existed for commit_size shape** |
| `dbt test --select silver.events_push` | **FAIL** | `not_null_events_push_commit_size`: Got 200 results, configured to fail if != 0 |

**Time-to-detection**: end of the test stage, i.e. immediately after
Silver build completes. In wall-clock terms, ~30 s on the local
sample. In a real DAG order
(bronze → gate → silver → gate → gold → gate → test_all), the
poisoning would have propagated through Gold before detection. That
is the actual operational risk this postmortem addresses.

## 5 Whys

1. **Why did the bad data reach dbt test stage instead of being
   stopped at the silver gate?**
   Because the silver gate (`quality/checks.py`) tested row counts
   between Bronze and Silver, but not the *contents* of the
   payload-derived columns. NULLs count toward row counts.

2. **Why did the silver gate not test payload-derived contents?**
   Because the Sprint 4 gate design focused on cross-layer cohesion
   (count match, schema-set membership) on the assumption that
   per-column null-ness was the dbt schema test's job. That assumption
   is true only if dbt tests are wired to fail Airflow tasks earlier
   than `dbt_test_all` runs.

3. **Why is `dbt_test_all` the last task instead of running
   per-layer?**
   Because in Sprint 4 the DAG was designed for a single
   `dbt_test_all` at the end to keep the topology simple. The
   trade-off was "if a test fails after Gold builds, we already
   poisoned downstream readers." We accepted that trade-off
   provisionally; this incident is the empirical reason to revisit
   it.

4. **Why didn't anyone notice this trade-off cost was real before
   incident-0001?**
   Because no actual schema-break had occurred in the project's
   sample data. The decision was theoretical until tested.

5. **Why is "no schema-break has happened yet" enough to defer
   defenses?**
   It isn't. ADR-0001 explicitly cites schema-drift as the design
   horizon. The pipeline architecture (raw JSON in Bronze) is
   drift-tolerant, but the *gate* layer wasn't proving that
   tolerance was actionable at the right point. Theoretical
   tolerance ≠ tested defense.

## Root cause

The Silver quality gate verified that data *flowed* (Bronze rows →
Silver rows, 1-to-1) but did not verify that data was *correct* for
the columns the downstream marts compute on. The dbt schema tests
DO verify correctness — but only at the very end of the pipeline,
after Gold is already built from the bad Silver.

This is a gate-placement problem, not a missing-test problem.

## Mitigations applied

1. **Hot fix in the model** — `events_push.sql` `commit_size` now
   does `coalesce(size, commit_count)`. Historic rows (with `size`)
   and post-rename rows (with `commit_count`) both populate. Verified
   by re-running `dbt test --select silver.events_push` against the
   incident partition: 10/10 PASS.
2. **New regression gate** at the silver layer in `quality/checks.py`:
   `silver_commit_size_not_null` — fails the silver gate if any
   `events_push.commit_size` is NULL. This moves detection from
   end-of-pipeline to mid-pipeline, before Gold is built.
3. **Cleanup** — `incident_inject.py --cleanup` removes the synthetic
   partition. The repo is now back to the pre-incident state with
   the fixes in place.

## Recovery

For an actual incident with this signature, the recovery would be:

```bash
# 1. Hot-fix Silver model with coalesce (above)
# 2. Full-refresh affected silver model
cd dbt && uv run dbt run --select silver.events_push --full-refresh
# 3. Full-refresh any Gold mart that reads commit_size
cd dbt && uv run dbt run --select gold.repo_daily_activity --full-refresh
# 4. Run all gates + tests
uv run python -m quality.runner --layer silver
uv run python -m quality.runner --layer gold
uv run python -m quality.runner --layer cross_mart
cd dbt && uv run dbt test
```

For this drill, full-refresh wasn't strictly required since we
cleaned up the synthetic partition entirely. The drill steps mirror
the production runbook in
[docs/runbooks/schema_change.md](../runbooks/schema_change.md).

## Preventive measures (longer-horizon)

| Action | Owner | When |
|--------|-------|------|
| New regression gate in silver suite (incident-0001) | applied | this PR |
| Move `dbt test --select silver` into Airflow as `gate_dbt_silver` between `build_silver` and `build_gold` | open | Sprint 4 follow-up |
| Same for `dbt test --select gold` between `build_gold` and `gate_cross_mart` | open | Sprint 4 follow-up |
| Add a contract test: "for each Silver model, the union of paths it `get_json_object`s exists in the corresponding event_type sample" | open | future Sprint — would have caught this even before the silver build ran, but adds a contract step that wasn't in the original plan |
| Document the gate-placement decision in ADR (which layer of the medallion runs which dbt tests when) | open | Sprint 5 follow-up |

## Lessons learned

1. **Schema-drift tolerance is a Bronze property, not a Silver one.**
   Bronze swallowed the rename without complaint, exactly as
   designed. The Silver layer needs its own drift-detection — being
   "downstream of a drift-tolerant Bronze" is not the same as being
   drift-tolerant itself.
2. **Gate placement matters more than gate count.** Adding more tests
   at the end of the pipeline doesn't prevent poisoning — it only
   detects it after the fact. The valuable gate is the one between
   the failing layer and the next layer.
3. **A deliberate-incident drill is the only way to prove the
   detection chain.** Five different things in this report I would
   have got wrong from theory alone:
   - I expected the silver gate's row-count-match to fail (it didn't —
     NULLs are still rows)
   - I expected `gate_bronze` to be relevant (it wasn't — payload-shape
     isn't checked at Bronze by design)
   - I expected dbt's `on_schema_change='fail'` to catch it (it
     doesn't — the failure mode is row contents, not column shape)
   - I expected the fix to be a Silver model change only (it also
     required a new gate — model fix without gate fix would leave the
     same gap for the next rename)
   - I expected the incident to be obvious from logs (it wasn't — the
     200 violating rows are buried in a 385,321-row table)
4. **One incident, one regression check.** Every postmortem leaves
   behind at least one new check the next incident of the same
   shape would fail on. Otherwise the postmortem is theatre.

## References

- ADR-0001 — Bronze payload handling (why payload is raw JSON STRING)
- [docs/runbooks/schema_change.md](../runbooks/schema_change.md) —
  general-case runbook for upstream schema changes
- `quality/checks.py::silver_commit_size_not_null` — the regression
  check born from this incident
