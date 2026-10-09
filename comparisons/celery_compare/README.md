# Order fulfilment on Celery + RabbitMQ

The same six steps and the same fake vendors as `python -m demo run`, written the two ways
teams usually write them. Compare what `python -m demo ledger` shows after each.

```bash
# once, while you have wifi
docker compose -f comparisons/celery_compare/docker-compose.yml pull
uv venv comparisons/celery_compare/.venv && uv pip install -r comparisons/celery_compare/requirements.txt

# every time, from the repository root
python -m demo vendors                                                   # terminal 1
docker compose -f comparisons/celery_compare/docker-compose.yml up -d    # RabbitMQ
CRASH_AT=order-0007:reserve_inventory \
  comparisons/celery_compare/.venv/bin/python -m celery -A comparisons.celery_compare.tasks worker -P threads -c 1
comparisons/celery_compare/.venv/bin/python -m comparisons.celery_compare.submit one 10    # or: chain 10
python -m demo ledger
```

`CRASH_AT=order-0004:reserve_inventory[:after]` SIGKILLs the worker once (a marker file stops it firing again). Use `-P threads -c 1` on macOS (see below).
Restart the worker and RabbitMQ redelivers the unacked message.

* **`one`**: one task per order. Redelivery or a retry restarts the order at step 1, so orders
  are charged, reserved, labelled and emailed again.
* **`chain`**: one task per step. Only the interrupted step repeats, which is a real improvement.
  The remaining gaps are in `tasks.py`: no audit trail, no skip-if-done after a side effect,
  no provider fallback or breaker, no "continue everything unfinished".

## What we measured

6 orders, `--chaos calm`, one worker killed once, then restarted. RabbitMQ 3.13, Celery 5.6,
`acks_late=True`, `-P threads -c 1` (see the note below on pools).

| crash point | style | result in the vendors' ledger |
|---|---|---|
| before `reserve_inventory` of order 4 | `one` | order 4 charged **twice** (the whole order restarted) |
| before `reserve_inventory` of order 4 | `chain` | nothing duplicated: only the interrupted step reran |
| after `capture_payment` of order 4, before the ack | `chain` | order 4 charged **twice** |
| same crash | stitch, no idempotency key | order 4 charged **twice** |
| same crash | stitch, with its idempotency key | charged once (the provider replayed the first charge) |

So the honest summary: a chain of Celery tasks is real step-level resume, and it handles a crash
*before* a step's side effect fine. Neither it nor stitch can protect a crash *after* the side
effect without an idempotency key; the difference is what each gives you around that: stitch
records what ran (`python -m demo trace`), skips finished steps without asking the vendor, can
fall back between providers, and can say "continue everything unfinished".

## Pool notes (macOS)

* `-P prefork` (the default) failed on this Mac with `ValueError: not enough values to unpack`
  in `fast_trace_task`: the pool child never ran its initialiser. On Linux it is the normal choice.
* `-P solo` makes the numbers misleading: the event loop is blocked while a task runs, so acks
  for finished tasks are not sent before the kill and many unrelated orders run twice. That run
  duplicated reservations, labels and emails for every order. Do not quote it.
* `-P threads -c 1` acknowledges promptly and gives the table above.
