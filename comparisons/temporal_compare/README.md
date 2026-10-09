# Order fulfilment on Temporal

The same six steps and the same fake vendors as `python -m demo run`, written the Temporal way:
activities (the steps with side effects), a workflow that calls them in order, a worker, and a
starter. Temporal makes the workflow durable by recording every activity result in its event
history and replaying that history after a crash.

```bash
# once, while you have wifi
docker compose -f comparisons/temporal_compare/docker-compose.yml pull
uv venv comparisons/temporal_compare/.venv && uv pip install -r comparisons/temporal_compare/requirements.txt

# every time, from the repository root
python -m demo vendors                                                      # terminal 1
docker compose -f comparisons/temporal_compare/docker-compose.yml up -d     # UI: http://localhost:8233
CRASH_AT=order-0004:capture_payment:after IDEMPOTENT=0 \
  comparisons/temporal_compare/.venv/bin/python -m comparisons.temporal_compare.worker   # dies once
comparisons/temporal_compare/.venv/bin/python -m comparisons.temporal_compare.start 6    # another terminal
# start the worker again (without CRASH_AT) and watch the workflows finish
python -m demo ledger
```

## What we measured

6 orders, `--chaos calm`, one worker killed once and restarted, Temporal dev server in Docker,
`temporalio` Python SDK, one activity at a time.

| crash point | idempotency key | result in the vendors' ledger |
|---|---|---|
| before `reserve_inventory` of order 4 | none | every order charged once |
| after `capture_payment` of order 4 | none | order 4 charged **twice** |
| after `capture_payment` of order 4 | workflow id + activity id | every order charged once (the provider replayed the charge) |

This matches Celery's chain and stitch: **durable execution does not remove the need for
idempotency.** Activities are at-least-once. What the engine gives you is that nothing before
the interrupted activity is redone, and you can see why in the UI.

## Concurrency changes the blast radius

The first attempt let the worker run all six workflows' activities at once. When it was killed,
every order that happened to be mid-call lost its result, and 5 of 6 orders were charged twice,
even for a crash planned for another step. The cause is at-least-once delivery plus concurrency,
not a Temporal fault, and the same applies to Celery with many threads and to stitch with
`--concurrency 20`. Idempotency keys are what make that safe. The table above uses
`max_concurrent_activities=1` (`ACTIVITY_CONCURRENCY` overrides it) so the three tools are
compared at the same concurrency.

## What Temporal gives that stitch does not

Distributed workers, durable timers and signals, workflow versioning, a replay-based
guarantee that survives losing the server's workers entirely, a history UI, and multi-language
SDKs. What it asks in return: a server (and database) to run, a deterministic workflow sandbox
with rules about what workflow code may do, and a worker deployment story.
