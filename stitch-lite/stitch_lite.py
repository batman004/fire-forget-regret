import asyncio
import contextvars
import functools
import json
import logging
import os
import random
import sqlite3
import sys

logger = logging.getLogger("stitch_lite")
current_item = contextvars.ContextVar("current_item", default=None)
health_checks = {}


class Halted(Exception):
    def __init__(self, message, step_name):
        super().__init__(message)
        self.step_name = step_name


class Store:
    def __init__(self, path):
        self.database = sqlite3.connect(path, isolation_level=None)
        self.database.executescript(
            "PRAGMA journal_mode=WAL;"
            "CREATE TABLE IF NOT EXISTS items "
            "(item TEXT PRIMARY KEY, input TEXT, done INTEGER DEFAULT 0);"
            "CREATE TABLE IF NOT EXISTS steps "
            "(item TEXT, step TEXT, seq INTEGER, status TEXT, result TEXT, PRIMARY KEY (item, step));"
            "CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT);"
        )

    def close(self):
        self.database.close()

    def first(self, sql, *parameters):
        return self.database.execute(sql, parameters).fetchone()

    def execute(self, sql, *parameters):
        self.database.execute(sql, parameters)

    def start_item(self, item, value):
        self.execute(
            "INSERT INTO items (item, input) VALUES (?, ?) ON CONFLICT (item) DO NOTHING",
            item,
            json.dumps(value),
        )

    def is_done(self, item):
        row = self.first("SELECT done FROM items WHERE item = ?", item)
        return bool(row and row[0])

    def saved_step(self, item, step):
        return self.first("SELECT result FROM steps WHERE item = ? AND step = ?", item, step)

    def save_step(self, item, step, sequence, status, result):
        self.execute(
            "INSERT OR REPLACE INTO steps VALUES (?, ?, ?, ?, ?)",
            item,
            step,
            sequence,
            status,
            json.dumps(result),
        )

    def forget_from_first_fallback(self, item):
        self.execute(
            "DELETE FROM steps WHERE item = ? AND seq >= "
            "(SELECT MIN(seq) FROM steps WHERE item = ? AND status = 'fallback')",
            item,
            item,
        )
        self.execute("UPDATE items SET done = 0 WHERE item = ?", item)
        return json.loads(self.first("SELECT input FROM items WHERE item = ?", item)[0])

    def items_with_fallback(self):
        rows = self.database.execute("SELECT DISTINCT item FROM steps WHERE status = 'fallback'")
        return [row[0] for row in rows]


class ItemState:
    def __init__(self, key, store):
        self.key, self.store, self.calls = key, store, {}

    def next_step(self, name):
        self.calls[name] = self.calls.get(name, 0) + 1
        step_id = name if self.calls[name] == 1 else f"{name}#{self.calls[name]}"
        return step_id, sum(self.calls.values())


def delay_before_retry(error, attempt, backoff):
    hint = getattr(error, "retry_after", None)
    return hint if hint is not None else backoff * 2 ** (attempt - 1) * random.uniform(0.5, 1)


async def call_with_retries(function, arguments, retries, backoff, retry_on):
    for attempt in range(1, retries + 1):
        try:
            return await function(*arguments)
        except retry_on as error:
            if attempt == retries:
                raise
            logger.warning("%s failed (%s), retry %s/%s", function.__name__, error, attempt + 1, retries)
            await asyncio.sleep(delay_before_retry(error, attempt, backoff))


def step(retries=4, backoff=None, retry_on=(OSError,), fallback=None, critical=None):
    backoff = backoff or float(os.environ.get("STITCH_BACKOFF", 1.0))

    def decorate(function):
        health_checks[function.__name__] = critical

        @functools.wraps(function)
        async def checkpointed_step(*arguments):
            state = current_item.get()
            if state is None:
                return await function(*arguments)
            step_id, sequence = state.next_step(function.__name__)
            saved = state.store.saved_step(state.key, step_id)
            if saved:
                return json.loads(saved[0])
            try:
                result = await call_with_retries(function, arguments, retries, backoff, retry_on)
                status = "done"
            except retry_on as error:
                if critical:
                    message = f"{function.__name__} failed for good: {error}"
                    raise Halted(message, function.__name__) from error
                if fallback is None:
                    raise
                result, status = fallback(*arguments), "fallback"
            state.store.save_step(state.key, step_id, sequence, status, result)
            return result

        return checkpointed_step

    return decorate


async def process_item(handler, value, store, key):
    item = key(value)
    if store.is_done(item):
        logger.info("[%s] already finished, skipped", item)
        return
    store.start_item(item, value)
    token = current_item.set(ItemState(item, store))
    try:
        await handler(value)
        store.execute("UPDATE items SET done = 1 WHERE item = ?", item)
    except Halted:
        raise
    except Exception as error:
        logger.error("[%s] stopped: %s. Run again to continue.", item, error)
    finally:
        current_item.reset(token)


async def as_async_iterator(items):
    if hasattr(items, "__aiter__"):
        async for value in items:
            yield value
    else:
        for value in items:
            yield value


async def is_healthy(check):
    try:
        return await asyncio.to_thread(check) is not False
    except Exception:
        return False


async def require_healthy_resource(store, alert):
    halted = store.first("SELECT value FROM meta WHERE key = 'halted'")
    if halted is None:
        return
    if not await is_healthy(health_checks[halted[0]]):
        raise Halted(f"{halted[0]} is still unhealthy, not starting", halted[0])
    store.execute("DELETE FROM meta WHERE key = 'halted'")
    alert("recovered", f"{halted[0]} is healthy again, the run resumes")


def log_alert(kind, message):
    (logger.critical if kind == "halted" else logger.info)("ALERT %s: %s", kind, message)


def item_key(value):
    return str(value["id"] if isinstance(value, dict) else value)


def as_store(store):
    return store if isinstance(store, Store) else Store(store)


async def run(handler, items, store="stitch.db", key=item_key, workers=5, alert=log_alert):
    store = as_store(store)
    await require_healthy_resource(store, alert)
    limit = asyncio.Semaphore(workers)
    try:
        async with asyncio.TaskGroup() as tasks:
            async for value in as_async_iterator(items):
                await limit.acquire()
                task = tasks.create_task(process_item(handler, value, store, key))
                task.add_done_callback(lambda finished: limit.release())
    except BaseExceptionGroup as group:
        halted, _ = group.split(Halted)
        if halted is None:
            raise
        failure = halted.exceptions[0]
        store.execute("INSERT OR REPLACE INTO meta VALUES ('halted', ?)", failure.step_name)
        alert("halted", str(failure))
        raise failure from None


async def heal(handler, store="stitch.db", key=item_key, workers=5, alert=log_alert):
    store = as_store(store)
    inputs = [store.forget_from_first_fallback(item) for item in store.items_with_fallback()]
    await run(handler, inputs, store, key, workers, alert)


def run_until_halted(coroutine):
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s")
    try:
        asyncio.run(coroutine)
    except Halted as halted:
        logger.critical("pipeline halted: %s", halted)
        sys.exit(2)
