# ADR-0006: Bot identification — Rule A (login suffix) + Rule C (curated allowlist) + event-level app flag

- **Status**: Accepted
- **Date**: 2026-06-28
- **Deciders**: Peter Wang
- **Tags**: bot-detection, modeling, gold, silver
- **Codified from**: Sprint 2.5 spike
  (`docs/spikes/bot_heuristic.md`) and Sprint 3b implementation
  (`dbt/seeds/known_bots.csv`, `dbt/macros/is_bot.sql`,
  `dbt/models/gold/bot_vs_human_activity_mart.sql`)

## Context

The Bot vs Human Activity Mart needs a rule to classify each event
as bot-originated or not. The original Sprint plan proposed two
rules:

- **Rule A** — `actor.login` ends with the literal `[bot]`
- **Rule B** — `payload.performed_via_github_app` is non-null

Sprint 2.5's spike (`docs/spikes/bot_heuristic.md`) ran these against
613,876 real Bronze events. Key findings:

1. **Rule B as originally framed is wrong.** The `performed_via_github_app`
   field does not exist at the payload root. It lives on sub-objects
   (`issue.*`, `comment.*`, `pull_request.*`, `review.*`). The naive
   `$.performed_via_github_app` query returns 0 hits.
2. **Once the path is fixed, Rule B's incremental value is mostly
   noise.** It adds only 40 events over Rule A (0.024 %), and the
   27 actors it adds beyond Rule A are *humans using a GitHub App*,
   not bots. Rule B is measuring "was this event posted via an app",
   which is not the same question as "is this actor a bot".
3. **Rule A on its own catches 7 of 8 visible bots in the top 20
   by event count (87.5 %).** Below the 90 % threshold the spike was
   set up to defend. The miss (`LombiqBot`) is a custom-named bot
   that doesn't follow GitHub's `[bot]` convention.
4. **There is an unclassifiable "uncertain" bucket** —
   high-volume single-actor accounts (`frdpzk2`, `zacw-243L`, ...)
   that look scripted but no rule can prove it without external
   signal.

## Decision

The project adopts a three-piece rule set, used as a UNION
("an event is bot-originated if any of these is true"):

- **Rule A** — `actor.login LIKE '%[bot]'`. High precision (`[bot]` is
  GitHub-imposed). Carries the bulk of the recall.
- **Rule C** — `actor.login` appears in
  `dbt/seeds/known_bots.csv` (curated allowlist). Catches bots that
  don't follow the suffix convention. Lives in version control so
  additions go through PR review.
- **Event-level `is_app_event` flag** — separate from bot
  classification. Surfaced in Silver per-event-type tables where
  meaningful (Sprint 3a's `events_issue_comment.is_app_event` is the
  current example). The Bot mart reports `bot_actor_share` and
  `app_event_share` as two different columns.

`is_bot(actor_login)` is implemented as a dbt macro
(`dbt/macros/is_bot.sql`) that joins Rule A and Rule C into a single
boolean expression every mart can reuse without duplicating SQL.

The mart additionally exposes an `actor_class` column with values:
- `bot` — caught by Rule A or Rule C
- `human` — not caught, AND `actor_login` is non-null AND not in the
  uncertain bucket (currently: every non-bot is `human`; the
  uncertain bucket is documented in the spike but not yet a separate
  class — see "Future" below)
- `unknown` — `actor_login` is null (rare; defensive only)

## Rejected: Rule B (`performed_via_github_app` as a bot rule)

Documented in Sprint 2.5 spike. The field is event-level and means
"posted via a GitHub App", not "actor is a bot". A maintainer
commenting from the GitHub mobile app would trigger it. Keeping it
as a bot rule would silently overcount bots and undercount humans.

The same field IS useful as an event-level signal, surfaced in
Silver (`events_issue_comment.is_app_event` is the first instance)
and reported separately in the Bot mart.

## Future: Rule D (anomalous-cadence detection)

The "uncertain bucket" from the spike (`frdpzk2`, `zacw-243L`,
single-actor accounts with 1,000+ pushes in a day) is not classified
by A or C. A heuristic like "actor pushed > N times in M hours from
< K distinct repos" would flag many of them. This is rule-based
behavioral detection, in the spirit of the project's
"no ML" boundary in ADR-0001's scope notes.

Not implementing in Sprint 3 because:

1. Defining N, M, K defensibly needs cross-day data; the current
   4-hour sample window is too narrow to set thresholds.
2. The Bot mart can already tell the story without it (the
   percentage of "uncertain" actors is reportable as a known gap).

Revisit when Sprint 4's Airflow DAG has backfilled at least one full
week of data.

## Allowlist governance

`dbt/seeds/known_bots.csv` schema: `login,note,added_date,added_by`.

- Adding an entry: PR with a one-line justification in the `note`
  field. Reviewer must check the GitHub profile and confirm it's an
  account that posts in an automated way.
- Removing an entry: PR. Note the reason in the commit message
  (e.g. "renamed to suffix `[bot]`, now caught by Rule A").
- Initial seed: `LombiqBot` (the one visible miss from the spike).
  Future Sprint can grow it from `frdpzk2`-style actors once their
  bot-ness is confirmed.

## Consequences

**Positive**:
- Single source of truth for bot classification (`is_bot()` macro),
  reused by every mart that needs it.
- Allowlist is auditable in git history. Adding 1 entry is 1 line of
  CSV, not a code change.
- `app_event_share` is preserved as a separate, honest metric instead
  of being conflated with bot-actor share.
- The "uncertain bucket" is acknowledged rather than silently merged
  into `human` (which would bias the bot-share metric downward).

**Negative**:
- Rule A's 87.5 % recall on the spike's top-20 is honestly below the
  initially-stated 90 % bar. The decision to ship at 87.5 % is
  defensible because the miss is fixable by Rule C and broader
  pattern matching would create false positives.
- Allowlist drift: an unmaintained `known_bots.csv` will silently
  underclassify over time. Sprint 4's runbooks will include
  "review known_bots quarterly".
- Rule D not implemented → some scripted/spam accounts will be
  classified as `human` in the current mart. The mart should not be
  used as a final authority on bot share; it's a defensible upper
  bound on humans, lower bound on bots.

## Status conditions for revisit

Re-open this ADR when **any one** of:

1. The first full-week backfill (post Sprint 4) lets Rule D be
   tuned with confidence.
2. GitHub introduces an API field that authoritatively marks bot
   accounts (Status would become Superseded, not edited).
3. A new event type's payload exposes a strong bot signal not yet
   considered.

## References

- `docs/spikes/bot_heuristic.md` — empirical evidence
- ADR-0001 (Bronze payload handling) — payload-shape rationale that
  led to the Rule B path-correction
- `dbt/macros/is_bot.sql` — shared implementation
- `dbt/seeds/known_bots.csv` — Rule C allowlist
- `dbt/models/gold/bot_vs_human_activity_mart.sql` — first
  consumer
