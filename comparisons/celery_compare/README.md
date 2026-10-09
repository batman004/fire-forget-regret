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
  comparisons/celery_compare/.venv/bin/python -m celery -A comparisons.celery_compare.tasks worker -c 1
comparisons/celery_compare/.venv/bin/python -m comparisons.celery_compare.submit one 10    # or: chain 10
python -m demo ledger
```

`CRASH_AT` SIGKILLs the worker once at that point (a marker file stops it firing again).
Restart the worker and RabbitMQ redelivers the unacked message.

* **`one`**: one task per order. Redelivery or a retry restarts the order at step 1, so orders
  are charged, reserved, labelled and emailed again.
* **`chain`**: one task per step. Only the interrupted step repeats, which is a real improvement.
  The remaining gaps are in `tasks.py`: no audit trail, no skip-if-done after a side effect,
  no provider fallback or breaker, no "continue everything unfinished".

Status: these files have not been run yet. The RabbitMQ image download did not complete
when this was written, so the commands above are unverified.
