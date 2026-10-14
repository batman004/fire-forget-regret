# stitch-lite

The smallest durable core that still does the useful things, in one standard-library file: [stitch_lite.py](stitch_lite.py).

It has **184 code lines** (blank lines, comments and docstrings not counted; lines wrapped at about 100 columns).
`tests/test_stitch_lite.py` counts them and fails if the file grows past 185. The full
library, [stitch](https://github.com/batman004/stitch), is 1,671 code lines.

```python
from stitch_lite import heal, run, run_until_halted, step

@step(retries=4, backoff=1.0, retry_on=(OSError,))          # checkpointed and retried
async def download(article): ...

@step(fallback=quick_verdict)                               # flagged fallback when retries run out
async def judge(article, text): ...

@step(critical=database_is_healthy)                         # fails for good: halt the run and alert
async def save(article, verdict): ...

async def process(article):
    text = await download(article)
    await save(article, await judge(article, text))

await run(process, articles, "state.db")      # items with an "id" are keyed by it; alerts are logged
await heal(process, "state.db")                # redo only the fallbacks
# or: run_until_halted(run(...))               # logging set up, exit code 2 if a critical step halts
# optional: key=..., workers=5, alert=send_alert(kind, message); STITCH_BACKOFF=0.3 speeds up retries
```

## What it keeps
- A finished step is saved in SQLite before the next one starts. A rerun skips it and returns the saved result.
- Retry with exponential backoff and jitter. A `retry_after` attribute on the error sets the wait instead.
- `fallback`: after the last retry a flagged fallback is stored and the run goes on. `heal` redoes only those steps.
- `critical`: after the last retry the whole run halts, `alert("halted", ...)` is called once and the run is stored
  as halted. The next `run` calls the health check first and refuses to start until it passes, then calls
  `alert("recovered", ...)` and carries on from the checkpoints.
- Several workers over a plain or async iterable (a stream that never ends works).

## What it drops
Per-step timeouts, the provider pool and circuit breakers, the audit log and tracing, parking items until a
cooldown, item-level retries, schema versioning and a reporter interface. It has the same limits as the full
library: steps must be safe to run twice, one process drives one state file, and a step is matched by its name and
call order, so change a step's code and you should start a new state file.
