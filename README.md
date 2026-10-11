# Fire, Forget, Regret

Companion repository for the PyCon NL 2026 talk. It shows what breaks in a long-running pipeline, and how adding a
decorator makes it durable.

```
talk/     the live demo: the same pipeline twice, plain and durable
stitch/   the library behind the decorator: checkpoints, retries, fallbacks (standard library only)
tests/    unittest
TALK.md   what the talk promised, what the repo covers, and the run sheet
```

## The demo

A pipeline reads a security news feed, downloads each article, asks Claude to rate it, and publishes the result.

```bash
pip install -r requirements.txt        # loguru, for the logs
cp .env.example .env                   # add your ANTHROPIC_API_KEY

python -m talk.breach_pipeline reset
python -m talk.breach_pipeline run     # then kill -9 it, switch wifi off, or exhaust your Claude limit
python -m talk.breach_pipeline report  # what it published, duplicates, paid Claude calls

diff talk/breach_pipeline.py talk/breach_pipeline_durable.py     # everything durability adds
python -m talk.breach_pipeline_durable run       # same failures, now it resumes
python -m talk.breach_pipeline_durable heal      # replace fallback results once Claude is back
```

The run sheet with what to expect in each act is in [TALK.md](TALK.md).

## stitch in one screen

```python
from stitch import CheckpointStore, RetryPolicy, Workflow, step

@step(retry_policy=RetryPolicy(retry_on=(OSError,)))
async def download(article): ...

@step(degrade=quick_fallback)
async def analyse(article, text): ...

async def process(article):
    text = await download(article)
    await analyse(article, text)

store = CheckpointStore("state.db")
await Workflow(process, name="demo").run_many(articles, store)
```

Every completed step is saved before the next one starts, so a rerun skips what is done. A step is retried with
backoff; when retries run out, `degrade` supplies a flagged fallback instead of stopping the run, and
`WorkflowRecovery(store).heal()` redoes those steps later. Steps must be safe to run twice, because a crash can land
after a step's side effect but before its checkpoint is written. A workflow takes one JSON-serialisable argument and
one process drives one state file.

## Tests

```bash
python -m unittest
```

They cover the library and both demo scripts (kill and resume, internet down, Claude refusing, heal) with a fake
network. The demo tests are skipped if `loguru` is not installed. MIT licence.
