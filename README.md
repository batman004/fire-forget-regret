# Fire, Forget, Regret: Building Resumable Python Pipelines

Companion repository for the PyCon NL 2026 talk (Jaarbeurs Utrecht, 15 October 2026).

`stitch` is a small durable-execution library, standard library only, that you use as
a decorator on your long-running async functions. Kill the process halfway through,
start it again with the same command, and it continues from the last completed step.
It is a teaching-sized version of what Temporal, DBOS and Hatchet do: read it to
understand the idea, then decide which real tool your team needs.

```
stitch/          the library: ~1,600 lines, sqlite3 + asyncio, nothing to install
hook/          Part A: three ways a background asyncio task lives or dies
demo/          two workloads on top of stitch, plus fake third-party services
comparisons/   the same pipeline on Celery + RabbitMQ (optional, needs Docker)
tests/         unittest, Python 3.11 - 3.14
```

Requires Python 3.11+. Clone and run; nothing to install for `stitch`, `hook` or `demo`.

## The library in 30 lines

```python
import stitch
from stitch import Retry, Pool, step, workflow

stitch.configure("state.db")

llm = Pool("llm", factory=make_client, defaults=[("eu-west-1", 1, {...})])

@step(retry=Retry(attempts=4), timeout=8)                  # checkpointed, retried
async def enrich(event): ...

@step(degrade=rule_based_summary)                          # failsafe: fallback result
async def summarise(event, intel):
    return await llm.call(lambda c: c.complete(event))    # tries providers in order

@step
async def publish(event, summary): ...                     # must be safe to run twice

@workflow(key=lambda e: e["id"])                           # one durable unit of work
async def triage(event):
    intel = await enrich(event)
    summary = await summarise(event, intel)
    await publish(event, intel, summary)

await triage(event)                  # same key twice: the second call does nothing
await triage.map(events, concurrency=20)
await stitch.resume()                  # after a crash: continue everything unfinished
await stitch.heal()                    # after recovery: redo steps that used a fallback
```

What each piece does:

| capability | how |
|---|---|
| **Checkpointing** | each step's result is committed to sqlite before the next step starts; a rerun skips every recorded step |
| **Resume** | the workflow input is stored as JSON, so `resume()` restarts unfinished work with no caller and no queue |
| **Retries** | per step, exponential backoff with jitter, honours `Retry-After`; the unit of retry is the step |
| **Audit and tracing** | every state change is appended to an `audit` table; `render_trace()` turns it into one item's story |
| **Provider pool** | ordered, interchangeable providers (LLM regions, payment processors...) with failover |
| **Circuit breakers** | out of credits, rate limited, or failing: the provider cools down, then gets one probe call. Stored in sqlite, so a cooldown survives a restart |
| **Runtime registration** | the provider registry lives in the store; another process can add a provider and the next call uses it |
| **Failsafe** | `degrade=` produces a placeholder result so consumers get something instead of nothing; the item is flagged |
| **Cooldown retry** | with no fallback, the item is *parked* until the breaker reopens while the rest of the batch continues |
| **Heal** | redo the first degraded step and everything after it, once providers are back |
| **Memory** | items stream through a bounded queue; memory stays flat for 10 or 10 million items |

Constraints worth knowing: a workflow takes one JSON-serialisable argument; steps must
be safe to run twice (a crash can land after the side effect but before the checkpoint);
one process drives a store at a time. See "What it is not" below.

## Part A: the task that disappears

```bash
python -m hook          # all three acts, Enter between them
python -m hook 2        # one act
```

1. **Fire and forget, then the caller exits.** The loop shuts down and cancels the orphan.
2. **No reference held, loop still alive.** The task waits on a future only a weak
   registry points at; a garbage collection pass reclaims it and you get the real
   `Task was destroyed but it is pending!` warning. In production, memory pressure
   decides when.
3. **The fix.** Hold a strong reference (a set, or `asyncio.TaskGroup`) and await it.

Each act runs in its own `asyncio.run()`; sharing one loop would let act 1's task finish quietly.

## Part B: workload 1, order fulfilment (money moves)

Six steps, five of them calling a different fake vendor on localhost. The vendors keep
their own ledger: killing the pipeline does not undo a charge.

```bash
python -m demo vendors                    # terminal 2: --chaos calm|flaky|storm
python -m demo naive --crash-at order-0007:reserve_inventory   # no checkpoints
python -m demo naive
python -m demo ledger                     # orders charged twice, duplicate emails

python -m demo reset
python -m demo run --crash-at order-0007:reserve_inventory     # SIGKILL mid-run
python -m demo status                     # which steps are recorded
python -m demo run --crash-at order-0007:reserve_inventory     # same command: resumes
python -m demo ledger                     # every order charged exactly once
```

`--crash-at` fires only the first time, so rerunning the identical command resumes.
You can also `kill -9 <pid>` by hand; the pipeline prints its PID.

**The gap checkpoints can't close:** `--crash-at order-0002:capture_payment:after` kills the
process after the vendor charged but before the checkpoint. Add `--no-idempotency` and
the card is charged twice; leave it off and the payment provider replays the first charge
(idempotency key `run/item/step`).

## Part B: workload 2, security-event triage (LLM with fallbacks)

An LLM summarises and classifies security events for an analyst dashboard. What happens
when the account runs out of credits, or a region goes down? The fake LLM has regions, a
token budget, outages, rate limits and truncated answers.

```bash
python -m demo llm topup eu-west-1 700         # a small budget, so it runs out mid-batch
python -m demo triage --events 12              # events 1-4 get the LLM; then credits run out
python -m demo ui --of 12                      # dashboard: nothing stale, fallbacks marked
python -m demo trace evt-0005                  # why: quota error, breaker open, fallback used
python -m demo providers add us-east-1         # register a new region while things run
python -m demo heal                            # fallbacks replaced (dashboard v1 -> v2)
python -m demo ui --of 12
```

Other things to try:

```bash
python -m demo triage --on-outage park         # no fallback: wait out the cooldown, then retry
python -m demo llm topup eu-west-1 5000        # ...from another terminal, while it waits
python -m demo llm outage eu-west-1 on         # a regional outage instead of exhausted credits
python -m demo triage --events 200 --concurrency 20 --crash-at evt-0100:classify
python -m demo providers                       # live breaker state of each provider
```

## Comparison: the same pipeline on Celery and on Temporal

`comparisons/` runs order fulfilment on both, killing a worker mid-run. Measured with 6 orders:

| crash | Celery, task per order | Celery, chain | Temporal | stitch |
|---|---|---|---|---|
| before a step's side effect | whole order restarts: charged **twice** | step reruns: **once** | **once** | **once** |
| after the charge, no idempotency key | twice | twice | twice | twice |
| after the charge, with a key | n/a (not sent) | n/a (not sent) | once | once |

The lesson is the one in the talk: a queue's unit of retry is the job unless you split it into
a chain, and no tool, including a durable execution engine, removes the crash-after-side-effect
gap. Idempotency does. See each directory's README for how to run it (Docker, one `pip install`).
`stitch` adds an audit trail, provider fallbacks and degraded results on top; Temporal adds
distribution, timers and versioning that stitch deliberately does not have.

## Which one do I need?

| the work | use |
|---|---|
| short, disposable, fine to lose | `BackgroundTasks` / a referenced `asyncio` task |
| one unit of work that needs retries and its own workers | a task queue: Celery, ARQ, Dramatiq |
| many steps over minutes or hours where resume must mean resume | checkpointing, or a durable execution engine |
| the above across many machines, long timers, versioned code, strict guarantees | Temporal, DBOS, Hatchet, Restate |

## What it is not

`stitch` runs in one process against one sqlite file. It has no distributed workers, no
durable timers beyond the cooldown loop, no workflow versioning, no replay of nondeterministic
code, and no multi-writer story. Temporal and its relatives solve exactly those. The point
of the library is that the core idea, record each completed step and skip it on restart, fits
in a few hundred lines you can read in an afternoon.

## Tests

```bash
python -m unittest -v
```

They start a real vendor server, `SIGKILL` real subprocesses and check what the vendors
saw. Test state goes to a temp directory (`DEMO_STATE_DIR`).

## Licence

MIT
