# Fire, Forget, Regret: tracking sheet

PyCon NL 2026, Thursday 15 October, Jaarbeurs Utrecht. 25 minutes plus 5 for questions.

## The demo in this repo

`talk/ai_news_pipeline.py` is a plain pipeline: keep searching for fresh AI news through a local OpenSERP server, download each new URL, ask a local Ollama model (qwen3) whether it is a real breakthrough or just hype, store the verdict in MongoDB, publish the result. Real internet, a real local model, and `loguru`, `pymongo` and `openserp` (the only dependencies; `pip install -r requirements.txt`). The logs use loguru's defaults: INFO for each step, SUCCESS when something completes, WARNING for retries and fallbacks, ERROR when an article stops.
`talk/ai_news_pipeline_durable.py` is the same file with stitch added. Show the difference with:

```bash
diff talk/ai_news_pipeline.py talk/ai_news_pipeline_durable.py
```

Before the talk, have these running (none of them survives a reboot or a closed shell):

```bash
ollama serve                                          # and `ollama pull qwen3` once
mongod --config /opt/homebrew/etc/mongod.conf         # no --fork on macOS; or any MongoDB on MONGO_URL
docker run -d -p 7000:7000 karust/openserp serve      # search; talk to 127.0.0.1, not localhost (AirPlay owns it)
```

Run from the repo root: `python -m talk.ai_news_pipeline run`, `... report`, `... reset`
(the durable one also has `heal`). The run never ends by itself: `TALK_ARTICLES=6` stops after six articles, `TALK_PASSES=1` stops after one sweep of the 32 search queries, `TALK_MAX_AGE_DAYS=7` keeps only results whose snippet shows an age of seven days or less. `TALK_WORKERS` sets the workers (5); `TALK_RETRY_SECONDS=0.3` speeds up retries; `TALK_ENGINES` picks the search engines (duckduckgo,bing; Google gives a captcha).

### Run sheet

| act | do this | plain pipeline | with stitch |
|---|---|---|---|
| 1. kill it | `run`, then `kill -9 <pid>` when it says "asking qwen3" for the second article, then `run` again, then `report` | starts over from article 1: downloads and pays again, publishes duplicates | skips article 1, resumes article 2, no duplicates, no extra spend |
| 2. internet dies | turn off wifi while it runs | traceback, the whole run stops | retries with backoff, stops that article cleanly; wifi back, run again, it finishes |
| 3. the model refuses | stop Ollama, then `run` | crashes at the first article | retries, then publishes a flagged fallback and keeps going; start Ollama, `heal` upgrades them |
| 4. the database dies | stop `mongod`, then `run` | keeps working and stores nothing, or crashes on the first write | `store_in_mongo` is a critical step: after its retries the run halts, one simulated email and Slack alert goes out (also in `talk/output/alerts.jsonl`), exit code 2; running again is refused until Mongo answers, then a recovered alert and the run resumes from its checkpoints |
| 5. at scale | `python -m talk.ollama_cooldown_proxy`, then `OLLAMA_URL=http://localhost:11435/api/chat ... run` with the defaults | crashes at the first 429 | waits out each 2 minute cooldown using the `Retry-After` header: no fallbacks, no duplicates |

Verified, first pass, on 2026-10-13 with live OpenSERP (DuckDuckGo and Bing), a real local qwen3 and a local MongoDB: 8 and 14 article runs, one killed with kill -9 and rerun (7 skipped, 6 done, 0 duplicates, one Mongo document per article). Not run: a long unbounded run, and the short-cooldown proxy against this version. Warm qwen3 calls take about 6 seconds and Ollama serves one at a time by default. The unit tests use fakes (`tests/test_talk_pipeline.py`, `tests/test_talk_cooldown_proxy.py`).

One-week run, 2026-10-13 (`TALK_MAX_AGE_DAYS=7 TALK_PASSES=1`, 5 workers, live OpenSERP, qwen3, local MongoDB): the first pass found 76 articles and took 8.5 minutes, 75 done and 1 stopped by a 429 from the news site. Running the same command again skipped 56, processed 19 articles that had appeared in the meantime, and the one site still answered 429. End state: 94 articles, 94 Mongo documents, 94 model calls, 0 duplicates, 0 fallbacks; 16 labelled breakthrough and 78 hype. The labels are the model's: it called some funding and regulation stories a breakthrough, so do not present them as ground truth.

The durable script is the plain script plus 84 lines (`diff` counts them), most of them from line wrapping. Gaps to know about: a news site can answer 429 to the download, which stops that one article cleanly after four tries; the search results are live, so a rerun sees slightly different links; and the plain script's run is a `TaskGroup`, so its crash shows up as an exception group.

Say out loud: steps must be safe to run twice. A crash after `publish_result` wrote but before stitch recorded it
publishes once more. Here that is a duplicate line; in production use an idempotency key or an upsert.

## What the talk promised

| promise | where it came from | status | where / what is left |
|---|---|---|---|
| Rung 1: BackgroundTasks and fire-and-forget fail (no restart survival, no retries, no status) | abstract, social post | not in the repo | slides; the plain script shows "no survival" and "no status" for real |
| Rung 1 hook: an asyncio task can be garbage collected mid-execution | organizers' post | deleted in the cleanup | `git show HEAD:hook/act2_gc.py` (3 short stdlib scripts, tested); restore if you want it live |
| Rung 2: queues add workers and retries, but the unit of retry is the job: step 6 of 10 fails, you restart at step 1 | abstract, social post | evidence exists, not in the working tree | measured Celery and Temporal comparison in `git show HEAD:comparisons/celery_compare/README.md`; the plain script shows the same restart-from-one |
| Rung 3: durable execution persists every completed step | abstract, social post | done | `../stitch` plus the durable script; also retries, flagged fallbacks, critical steps with alerts |
| Live: real pipeline, kill it, restart, it resumes exactly | abstract, social post | done | `talk/`, rehearse the acts in the run sheet |
| Checkpointing library from the standard library alone | abstract, social post | done | `../stitch` has no dependencies at all |
| Close with how to choose: BackgroundTasks, queue or checkpointed pipeline, by job length | abstract, social post | slide only | draft below |

Decision framework draft: short and disposable work, use BackgroundTasks; one unit of work that needs retries and
workers, use a task queue; many steps over minutes or hours where resume must mean resume, use checkpointing or a
durable engine (Temporal, DBOS, Hatchet).

## What you told the organizers

| commitment | status |
|---|---|
| Live terminal demo on your laptop | done |
| No dependence on venue wifi | **conflict**: the demo needs internet for the search and the articles, plus a local Ollama with qwen3, a local MongoDB and a local OpenSERP in Docker. Your own act 2 needs wifi on first. Decide: rehearse on a hotspot, or record a replay of one real run |
| No third-party dependencies | `stitch` is standard library only. The demo scripts add three packages: `loguru` for logging, `pymongo` and `openserp` |
| Short screen recording as a backup | **not done**: the old recorder was deleted with the demos; record the acts once |
| Public demo repository | local only, not pushed; `.env` is git-ignored |
| Timing: illusion of background 5, queues 5, checkpointing 5, live demo 6, memory 2, framework and wrap 2 | the live demo should fit 6 minutes; four acts need tight rehearsal, and a warm qwen3 call takes about 6 seconds |
| Memory management segment (2 min) | **deleted** in the cleanup: `git show HEAD:demo/memory.py` (99 MiB list versus 0.4 MiB bounded stream). Keep it or drop the segment |
| Offer to adapt into a 105-minute workshop | nothing to do |
| General Python engineering, not an AI talk | present the model as "a slow, metered, flaky third party", and stitch as general-purpose |

Slides: the deck was updated on 2026-10-13 to this demo (pipeline, the three run slides, annotated snippets from the durable script, and a critical-step slide in the backup section). Still stale: the memory and compare backup slides cite numbers from deleted demos, and the Meta and Checkmate numbers have not been checked against their papers.

Not code, still on you: slides were due for review on 8 October, square photo (at least 800x800), roughly 100-word
third-person bio, share the news and tag PyCon NL (your ticket is a 100% discount code).
