# Fire, Forget, Regret: tracking sheet

PyCon NL 2026, Thursday 15 October, Jaarbeurs Utrecht. 25 minutes plus 5 for questions.

## The demo in this repo

`talk/breach_pipeline.py` is a plain pipeline: read a security news feed, download each article, ask Claude to
rate it, publish the result. Real internet, real Claude calls, and `loguru` for logging (the only dependency; `pip install loguru`). The logs use loguru's defaults: INFO for each step, SUCCESS when something completes, WARNING for retries and fallbacks, ERROR when an article stops.
`talk/breach_pipeline_durable.py` is the same file with stitch added. Show the difference with:

```bash
diff talk/breach_pipeline.py talk/breach_pipeline_durable.py
```

Run from the repo root: `python -m talk.breach_pipeline run`, `... report`, `... reset`
(the durable one also has `heal`). `TALK_ARTICLES=3` shortens a run; `TALK_RETRY_SECONDS=0.3` speeds up retries.

### Run sheet

| act | do this | plain pipeline | with stitch |
|---|---|---|---|
| 1. kill it | `run`, then `kill -9 <pid>` when it says "asking Claude" for article 2, then `run` again, then `report` | starts over from article 1: downloads and pays again, publishes duplicates | skips article 1, resumes article 2, no duplicates, no extra spend |
| 2. internet dies | turn off wifi while it runs | traceback, the whole run stops | retries with backoff, stops that article cleanly; wifi back, run again, it finishes |
| 3. Claude refuses | set a very low spend limit in the Anthropic console, then `run` | crashes at the first article | retries, then publishes a flagged fallback and keeps going; raise the limit, `heal` upgrades them |

Verified end to end with the real network and the real Claude API (kill -9, dead proxy for "no internet", bad key for
"Claude says no"), and by `tests/test_talk_pipeline.py` with fakes.

Say out loud: steps must be safe to run twice. A crash after `publish_result` wrote but before stitch recorded it
publishes once more. Here that is a duplicate line; in production use an idempotency key or an upsert.

## What the talk promised

| promise | where it came from | status | where / what is left |
|---|---|---|---|
| Rung 1: BackgroundTasks and fire-and-forget fail (no restart survival, no retries, no status) | abstract, social post | not in the repo | slides; the plain script shows "no survival" and "no status" for real |
| Rung 1 hook: an asyncio task can be garbage collected mid-execution | organizers' post | deleted in the cleanup | `git show HEAD:hook/act2_gc.py` (3 short stdlib scripts, tested); restore if you want it live |
| Rung 2: queues add workers and retries, but the unit of retry is the job: step 6 of 10 fails, you restart at step 1 | abstract, social post | evidence exists, not in the working tree | measured Celery and Temporal comparison in `git show HEAD:comparisons/celery_compare/README.md`; the plain script shows the same restart-from-one |
| Rung 3: durable execution persists every completed step | abstract, social post | done | `stitch/` plus the durable script |
| Live: real pipeline, kill it, restart, it resumes exactly | abstract, social post | done | `talk/`, rehearse the three acts |
| Checkpointing library from the standard library alone | abstract, social post | done | `stitch/` has no dependencies at all |
| Close with how to choose: BackgroundTasks, queue or checkpointed pipeline, by job length | abstract, social post | slide only | draft below |

Decision framework draft: short and disposable work, use BackgroundTasks; one unit of work that needs retries and
workers, use a task queue; many steps over minutes or hours where resume must mean resume, use checkpointing or a
durable engine (Temporal, DBOS, Hatchet).

## What you told the organizers

| commitment | status |
|---|---|
| Live terminal demo on your laptop | done |
| No dependence on venue wifi | **conflict**: the demo needs internet and the Claude API. Your own act 2 needs wifi on first. Decide: rehearse on a hotspot, or record a replay of one real run (the single `http_get` function is the one place to add it) |
| No third-party dependencies | `stitch` is standard library only. The demo scripts add one package, `loguru`, for logging |
| Short screen recording as a backup | **not done**: the old recorder was deleted with the demos; record the three acts once |
| Public demo repository | local only, not pushed; `.env` is git-ignored |
| Timing: illusion of background 5, queues 5, checkpointing 5, live demo 6, memory 2, framework and wrap 2 | the live demo should fit 6 minutes (three acts of about two minutes) |
| Memory management segment (2 min) | **deleted** in the cleanup: `git show HEAD:demo/memory.py` (99 MiB list versus 0.4 MiB bounded stream). Keep it or drop the segment |
| Offer to adapt into a 105-minute workshop | nothing to do |
| General Python engineering, not an AI talk | present Claude as "a slow, metered, flaky third party", and stitch as general-purpose |

Not code, still on you: slides were due for review on 8 October, square photo (at least 800x800), roughly 100-word
third-person bio, share the news and tag PyCon NL (your ticket is a 100% discount code).
