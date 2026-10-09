# Breach monitor: a long-running indexing job on stitch

One file, [pipeline.py](pipeline.py). It watches public security sources for data-breach news,
has Claude classify each breach's severity and explain it in plain language with an
entity/relationship graph, stores the result in MongoDB, and publishes the same event to a
RabbitMQ queue that feeds a downstream API.

```
sources ──► candidates (Mongo) ──► per article:
  RSS: BleepingComputer, The Record, Krebs,        fetch_article   plain HTTP; Chrome only with --browser
       The Hacker News, SecurityWeek,              analyse         Claude: breach? severity? (forced tool call)
       DataBreaches.net, CISA, r/cybersecurity     explain         Claude: summary, timeline, actions, graph
  APIs: Hacker News (Algolia), Have I Been Pwned   store_breach  ─┐ in parallel
                                                   publish_breach ┘ MongoDB upsert  +  RabbitMQ message
```

## Conduct

It uses public feeds and APIs, identifies itself in the `User-Agent` (set `BREACH_MONITOR_CONTACT`),
obeys `robots.txt`, and spaces requests to a host 2 s apart. When a site answers 403 or disallows
crawling, it analyses the feed summary instead and moves on; it does not work around it.
There is no fingerprint spoofing, stealth plugin, login, or underground-forum scraping, by design.
`--browser` runs headless Chrome (Selenium) only for pages whose text needs JavaScript, with the same honest
`User-Agent`.

Article text is untrusted input to the model. The system prompt says so, the model must answer through a
forced tool call, and its output is validated before it is stored; anything malformed is retried, never saved.

## Run it

```bash
cd examples/breach_monitor
docker compose up -d --wait                       # MongoDB + RabbitMQ (management UI: localhost:15672, guest/guest)
uv venv .venv && uv pip install -r requirements.txt
cp .env.example .env && chmod 600 .env            # then fill in ANTHROPIC_API_KEY and BREACH_MONITOR_CONTACT
                                                  # (.env is git-ignored; exported variables win over it)

.venv/bin/python pipeline.py doctor --sources-check   # config, services, and every source: spends nothing
.venv/bin/python pipeline.py run --max-items 10 --budget-usd 0.50
```

`--budget-usd` is a hard cap: once the estimated spend reaches it, Claude is treated like an
account that ran out of credits (see below). Defaults to Haiku 4.5 (`BREACH_MODEL` to change it),
at most two LLM calls per article.

| command | what it does |
|---|---|
| `run` | collect from every source, then process the newest articles |
| `status` | per-run counts, how many results are rule-based, MongoDB totals |
| `trace 10536d79` | the audit trail of one article: every step, retry, provider and fallback |
| `peek` | look at the queue without consuming it |
| `report` | recent breaches with severity, summary and graph edges |
| `resume` | continue everything unfinished or parked (also what to run after a crash) |
| `heal` | redo the articles that got a rule-based result, now that the LLM is back |
| `providers add backup --model claude-sonnet-5-5` | register a second model while a job is running |

## What stitch is doing here

* **Crash safety.** `kill -9` it mid-article and run the same command: the article is not downloaded
  again and no LLM call is repeated; it continues at the step that was interrupted.
  `run --crash-at <articleid>:analyse` demonstrates it.
* **Retries and politeness.** A 429 from a feed (Reddit does this) is retried with `Retry-After`; a source
  that stays down degrades to "0 candidates" and the other sources carry on.
* **LLM trouble never leaves the API stale.** Out of credits, rate limited, down, or over `--budget-usd`:
  the pool's circuit breaker opens, and the article is stored and published with a rule-based analysis
  flagged `analysis_source: "rule-based"`, `revision: 0`. After the cooldown, or after you top up,
  `heal` redoes just those steps, upserts revision 1, and publishes again. `--on-outage park` instead
  holds articles back until the breaker closes.
* **Safe to repeat.** Documents are upserted by id and a lower revision never overwrites a higher one.
  Queue messages carry `message_id = <id>:<revision>`, so a consumer dedupes redeliveries and sees
  upgrades as new revisions. Mongo or RabbitMQ down: the article is parked and retried, not lost.
* **Add capacity live.** `providers add backup --model claude-sonnet-5-5` from another terminal; the
  running job fails over to it on its next call.

## Message and document shape

Queue message (`type: breach.upserted`, persistent, publisher-confirmed):

```json
{"event": "breach.upserted", "id": "10536d791f018466", "revision": 1, "severity": "high",
 "analysis_source": "llm", "title": "...", "published": "2026-10-08T...", "doc": { ...full document... }}
```

The `breaches` collection holds the same document: `organization`, `sector`, `records_affected`,
`data_types`, `attack_vector`, `threat_actor`, `severity` (`low|medium|high|critical`) with
`severity_rationale`, `plain_summary`, `timeline`, `who_is_affected`, `what_to_do`, and
`graph: {nodes: [{id, label, type}], edges: [{source, target, label}]}` ready to render as a node graph.

## Honest limits

* Severity and the extracted facts come from a model reading a news article. Treat them as triage,
  and check `evidence` and the source link before acting.
* The rule-based fallback is deliberately crude; that is why it is flagged and healable.
* Candidate selection is a keyword filter on titles and summaries, so it misses breaches whose headlines
  do not use common words, and it will pass some irrelevant articles (the LLM then rejects them).
* One process drives one `breach_monitor.db`. Run it from a scheduler, not as many parallel copies.
* Candidate dedupe is by URL, not by event: two outlets covering the same breach both get published.
* The checkpoints and the sinks are separate stores. If you wipe MongoDB, wipe the state too, or stitch will
  (correctly, by its records) say everything is already processed:
  `docker compose down -v && find . -maxdepth 1 -name 'breach_monitor.db*' -delete`.
* Measured on the first real run: 8 articles, 12 Haiku 4.5 calls, about $0.07; 4 of 8 were breach reports.
