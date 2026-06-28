# Spike: bot identification heuristics (Sprint 2.5)

- **Date**: 2026-06-28
- **Purpose**: validate the bot-identification rules proposed for
  ADR-0006 (Sprint 3) *before* the Bot mart is built, so we don't
  rework Sprint 3 after the fact.
- **Bronze used**: 4 ingest_hours × 2015/2018/2025 sample,
  total 613,876 events, 148,587 distinct actors.
- **Script**: [`spark/jobs/bot_heuristic_spike.py`](../../spark/jobs/bot_heuristic_spike.py)
- **Raw output**: [`bot_heuristic_raw.txt`](./bot_heuristic_raw.txt)

## Rules tested

- **Rule A** — `actor.login` ends with the literal string `[bot]`
- **Rule B** — `payload.performed_via_github_app` is non-null. Because
  the field is *not* at the payload root in real GH Archive data, the
  spike probes 5 candidate paths:
  `$.performed_via_github_app`,
  `$.issue.performed_via_github_app`,
  `$.comment.performed_via_github_app`,
  `$.pull_request.performed_via_github_app`,
  `$.review.performed_via_github_app`.

The first iteration of the spike checked only the root path and
returned 0 hits. The expanded probe is what produced the numbers below.
**This shape detail is the first finding worth recording for ADR-0006**:
the field lives on sub-objects, not on the payload envelope.

## Results

| Metric                              | Rule A      | Rule B    | A ∪ B       |
|-------------------------------------|-------------|-----------|-------------|
| Events flagged                      | 166,939     | 7,792     | 166,979     |
| Events flagged by *both* rules      | —           | —         | 7,752       |
| Distinct actors flagged             | 631         | 324       | 658         |
| Share of all events                 | 27.19 %     | 1.27 %    | 27.20 %     |
| Share of all distinct actors        | 0.42 %      | 0.22 %    | 0.44 %      |

Rule B's incremental value over Rule A:
- **+40 events** (≈ 0.024 % of A's reach)
- **+27 actors** (≈ 4.3 % of A's reach)
- All but 8 of Rule B's events are `IssueCommentEvent`; the other 8
  are `IssuesEvent`.

## Top 20 actors by event count (manual eyeball)

| Rank | Login | Events | A | B | Manual label |
|------|-------|-------:|---|---|--------------|
| 1 | `github-actions[bot]` | 129,533 | ✅ | ✅ | bot |
| 2 | `renovate[bot]` | 6,480 | ✅ | ✅ | bot |
| 3 | `dependabot[bot]` | 6,203 | ✅ | ✅ | bot |
| 4 | `pull[bot]` | 4,491 | ✅ |   | bot |
| 5 | `swa-runner-app[bot]` | 3,635 | ✅ |   | bot |
| 6 | `frdpzk2` | 2,672 |   |   | uncertain |
| 7 | `zacw-243L` | 1,998 |   |   | uncertain |
| 8 | `Hall-1910` | 1,875 |   |   | uncertain |
| 9 | `CelestiaNFT` | 1,829 |   |   | uncertain |
| 10 | `direwolf-github` | 1,788 |   |   | uncertain |
| 11 | `LombiqBot` | 1,752 |   |   | **bot, missed by both rules** |
| 12 | `SoliSpirit` | 1,359 |   |   | uncertain |
| 13 | `sonarqubecloud[bot]` | 1,274 | ✅ | ✅ | bot |
| 14 | `iniadittt` | 1,257 |   |   | uncertain |
| 15 | `Lil-Seabas` | 1,254 |   |   | uncertain |
| 16 | `hotspotlab` | 1,192 |   |   | uncertain |
| 17 | `Alexey-Gorulev-kernelics` | 1,144 |   |   | uncertain |
| 18 | `blast0rama` | 1,124 |   |   | uncertain |
| 19 | `coderabbitai[bot]` | 1,108 | ✅ | ✅ | bot |
| 20 | `freecall2019` | 974 |   |   | uncertain |

Visible bots in top 20: **8** (the seven `[bot]`-suffixed accounts plus
`LombiqBot`). Rule A catches 7/8 → **87.5 %**. Below the 90 %
acceptance threshold the spike was set up to defend.

## Findings

1. **Rule B as written is the wrong abstraction.** It measures
   *"was this event posted via a GitHub App"*, not *"is this actor a
   bot"*. A human using GitHub Mobile, Linear's GitHub App, or
   Slack-integrated GitHub will trigger Rule B without being a bot.
   The 27 actors Rule B adds beyond Rule A are very likely
   human-using-app, not bot.
2. **Rule A is strong on its own.** 7 of 8 visible bots in the top 20
   are flagged. Recall on the easy cases is high.
3. **Rule A misses one important class**: bots whose login doesn't end
   with the literal `[bot]` suffix. `LombiqBot` is the example in this
   sample. A case-insensitive suffix match on `bot` would over-fire
   (`blast0rama` ends in `rama`, but other names like `robot`,
   `botanical-user` would be false positives).
4. **A large block of high-volume accounts (`frdpzk2`, `zacw-243L`,
   `Hall-1910`, …) cannot be classified without external lookup.**
   Their naming pattern (alphanumeric jumble) suggests scripted or
   spam activity, but the rule set has no signal for them.

## Decision input for ADR-0006

Recommended rule set when ADR-0006 is written in Sprint 3:

1. **Keep Rule A** (`actor.login` ends with `[bot]`) as the primary
   heuristic. Document its precision (high — `[bot]` is a GitHub-
   imposed convention for app-installed accounts) and known recall
   gap (custom-named bots like `LombiqBot`).
2. **Drop Rule B as originally framed.** Replace it with an
   *event-level* flag — `is_app_event` — derived from the same
   `performed_via_github_app` paths. This becomes a Bot-mart column
   ("share of events posted via apps") rather than a bot identifier.
3. **Introduce Rule C** (curated allowlist of known-bot names that
   don't use the `[bot]` convention). Start with `LombiqBot`. Keep
   the list in `dbt/seeds/known_bots.csv` so it's reviewable in PRs
   and doesn't require code changes.
4. **Acknowledge the "uncertain" bucket explicitly** in the mart
   schema yml. The Bot mart will categorize actors as
   `bot / human / uncertain`, not `bot / human`. This is honest about
   the limits of rule-based detection and a defensible interview
   answer ("I chose not to label unknowns as 'human' because that
   would silently bias the bot-share metric downward").

ADR-0006 (Sprint 3) will cite this spike as evidence. Sprint 2's
Gold mart `repo_daily_activity` uses Rule A only for its
`bot_push_count` / `non_bot_push_count` metrics; it does not need
Rules B or C because PushEvent is rarely from apps in this sample.

## Status

- Pass condition (≥ 90 % top-20-bot recall from A ∪ B): **failed at 87.5 %**.
  But the failure is informative: A alone hits 7/8; the miss
  (`LombiqBot`) is fixable with Rule C, not by changing A or B.
- Outcome: proceed to Sprint 2 Gold mart using Rule A only. ADR-0006
  in Sprint 3 will adopt A + C + an event-level `is_app_event` flag.
