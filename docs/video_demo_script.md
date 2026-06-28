# Video demo script — OSS Pulse

A reference for recording a 4–6 min walkthrough video to attach to a
job application or pin on LinkedIn. The goal is to land the seven
senior signals in a single watch, without the viewer needing to read
the repo.

## Recording tools (pick one)

| Tool | When to pick | Cost |
|------|--------------|------|
| **Loom** (web app) | Fast — no editing pass; ~1-click upload + share link | free tier covers 5 min |
| **QuickTime + iMovie** (macOS) | Want a clean cut, no Loom branding | free |
| **OBS Studio** | Need a webcam picture-in-picture + multi-source | free |
| **Screen Studio** (macOS) | Best polish — auto zoom on click, no editing | paid |

Recommended first pass: **Loom**, because it forces you to stay under
5 min and produces a shareable URL in seconds.

## Equipment

- External mic if you have one (USB lapel like RØDE NT-USB Mini, or
  any cheap condenser). Built-in laptop mic is fine if quiet room.
- Quiet room, no AC hum.
- 1080p screen capture. Above that = bigger files, no quality gain
  for code-heavy demos.

## Script (≈5 min, target ≈700 words spoken)

Below is a tight script. Each section names what's on-screen, then
the line to speak (≈110 words/min comfortable narration).

### 0:00 – 0:25 — Intro & one-line value prop
*On-screen: `README.md` open at top, GitHub repo home page.*

> "I'm Peter Wang. This is OSS Pulse — a production-grade GitHub
> activity lakehouse I built as a portfolio project for Senior Data
> Engineer roles. Six-hundred-thirteen-thousand events through Bronze,
> Silver, and Gold, gated by eighteen data-quality checks. Two real
> findings, one deliberately-injected incident, and a streaming MVP
> that reconciled to zero row delta against batch. I'll walk you
> through the senior-DE signals in five minutes."

### 0:25 – 1:15 — Architecture
*On-screen: scroll README to the Mermaid architecture diagram. Hover/zoom on each layer.*

> "Bronze keeps payload as raw JSON STRING — that's ADR-0001. It means
> upstream schema drift never crashes ingestion. Silver gets built
> only when a Gold mart needs it — ADR-0005, demand-driven. Three Gold
> marts: daily repo activity, OSS health, and bot vs human. Every
> layer MERGEs on stable GitHub ids — no surrogate keys, ADR-0004.
> Every irreversible decision is in `docs/adr/`."

### 1:15 – 2:00 — Real finding: bot vs human distribution
*On-screen: scroll to the bot-share pie chart in README, then briefly to
the top-bots text bar chart.*

> "Across a hundred-ninety-nine-thousand repo-days, the bot-vs-human
> distribution is bimodal. Sixty-six percent are pure human, thirty-two
> percent are pure bot, and only two-point-three percent are mixed.
> That two-point-three percent is the interesting tail. The original
> bot rule was just 'login ends with [bot]' — but the Sprint 2.5
> spike caught that one visible bot, LombiqBot, doesn't follow the
> convention. So ADR-0006 adopted a curated allowlist as Rule C, plus
> an event-level `is_app_event` flag that's deliberately kept separate
> from actor-level bot classification."

### 2:00 – 2:50 — The incident drill
*On-screen: scroll to the detection-chain Mermaid in README; open
`docs/postmortems/0001-schema-drift.md` briefly.*

> "Sprint 5b: I deliberately injected a schema break — renamed
> `payload.size` to `payload.commit_count` on two hundred events.
> The point wasn't 'find the bug'. The point was 'observe where the
> pipeline catches it'. Bronze ingest passed. The Bronze gate passed.
> The Silver build passed — `get_json_object` silently returns NULL.
> Even the Silver gate passed, because row counts still matched.
> The first detection was the dbt test step, at the *end* of the
> pipeline — by which point Gold would already have been poisoned.
> The fix was a one-line coalesce, plus moving a not-null check from
> end-of-pipeline to the silver gate. Lesson: gate-placement matters
> more than gate count. Every postmortem leaves a regression check."

### 2:50 – 3:30 — Streaming reconcile
*On-screen: terminal showing `docker-compose up`, then
`uv run python -m streaming.reconcile` output.*

> "Sprint 6 is the streaming MVP. Redpanda in Docker, a Python replay
> producer, a Spark Structured Streaming consumer using foreachBatch
> plus Delta MERGE for exactly-once writes. Reconciliation script
> compares row-by-row against batch silver for the same ingest hour.
> One hundred eighty-one thousand events, zero row delta. The
> exactly-once story is the natural idempotency of MERGE — no separate
> offset store, no extra moving parts."

### 3:30 – 4:15 — Cloud migration (live)
*On-screen: terminal showing `terraform output bronze_bucket`,
`aws s3 ls`, `uv run python -m spark.jobs.s3_smoke_test`.*

> "Sprint 5a is the cloud step. Terraform provisions three S3 buckets,
> public-access blocked, versioning on Bronze and Gold, with a sixty-day
> lifecycle to Infrequent Access for the cold tail. I uploaded the
> existing Bronze, and this is the smoke test reading it from S3
> through local Spark. Six-hundred-thirteen-thousand rows match
> exactly. The idempotency invariant survives the cloud round-trip.
> Total cost: about a dollar a month while running, zero after
> `terraform destroy`."

### 4:15 – 4:50 — The honest perf result
*On-screen: scroll README to the performance before/after ASCII
charts, then to `docs/performance/sprint5b_tuning.md`.*

> "Sprint 5b's performance tuning. The hypothesis was OPTIMIZE plus
> ZORDER on Bronze would prune per-type filter reads. The result:
> it didn't. The first wall-clock numbers showed a three-times
> speedup, but `inputFiles()` was the same. Four files before, four
> files after. The 'speedup' was JIT warmup, not data-skipping. I
> wrote that up as an honest negative finding. It also showed
> OPTIMIZE doubles storage until VACUUM runs — which is why ADR-0009,
> the VACUUM cadence ADR, isn't optional anymore."

### 4:50 – 5:10 — Wrap
*On-screen: README scroll-to-top.*

> "Seven ADRs accepted, five runbooks, one postmortem, eleven Delta
> tables, a hundred-plus dbt tests, eighteen DQ gates, a complete
> Airflow DAG, GitHub Actions CI, Terraform IaC, and a working
> streaming MVP. Repo link in the description. Thanks for watching."

## On-screen things to actually do (the visual track)

Order matters — if the terminal output isn't pre-captured, plan to
rehearse at least once:

1. **Open the README** at the top. Scroll smoothly past the
   architecture flowchart.
2. **Click into one ADR** (0001 or 0006 — both are short and dense).
3. **Switch to a terminal** with these commands ready (paste, don't
   type):

   ```bash
   uv run python -m streaming.reconcile --ingest-hour 2025-01-15-12
   ```

   It produces a clean 8-line output ending in `passed: True`.

4. **Switch back to README** for the postmortem section.
5. **`aws s3 ls s3://oss-pulse-bronze-dev-9f3eb8a5/events/`** as a
   live proof point. Have credentials already set in the shell.
6. **`uv run python -m spark.jobs.s3_smoke_test --bucket oss-pulse-bronze-dev-9f3eb8a5`**
   — ~30 s but reads from real S3.

If recording in Loom, keep the cuts. If editing in iMovie, trim Spark
JVM startup time (those 5-second pauses while jars resolve).

## Title + description for upload

**Title** (under 100 chars for YouTube SEO):
> OSS Pulse — production-grade GitHub activity lakehouse (Spark · Delta · dbt · Airflow · streaming)

**Description** (paste verbatim, edit links):

```
A 5-min walkthrough of OSS Pulse, my Senior Data Engineering
portfolio project: an end-to-end medallion lakehouse on PySpark +
Delta + dbt-spark, with idempotent ingest, 18 data-quality gates,
a parameterized Airflow DAG, a perf-tuning report with an honest
negative result, a deliberately-injected incident postmortem, and
a streaming MVP that reconciled 181,221 events against batch with
zero row delta.

Repo: https://github.com/MistFall-Wang/oss-pulse
Visual showcase: https://mistfall-wang.github.io/oss-pulse/

Chapters
0:00 Intro
0:25 Architecture
1:15 Bot vs human finding
2:00 The deliberate incident drill
2:50 Streaming reconcile
3:30 Cloud migration (live)
4:15 The honest perf result
4:50 Wrap
```

## Hosting

- **LinkedIn**: upload natively (it ranks native video higher) at
  1080p, captions on, with the chapters in the post body.
- **YouTube**: unlisted, paste the link into the README + LinkedIn.
- **Loom**: keep as a private link with collaborator access for
  recruiters who ask.

## What to NOT do

- No music. Distracting on technical content.
- No long static slides — scroll/click every 5-10s so the viewer
  feels progress.
- Don't apologize for anything. If a command takes 5s of JVM startup,
  cut it.
- Don't read the script word-for-word. Use it as a beat sheet; speak
  naturally in your own phrasing.
- No emoji or jokes in the narration. The work is dense enough; the
  delivery should be plain.
