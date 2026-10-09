"""The smallest useful stitch program: three steps, one workflow, survives a kill.

    python examples/quickstart.py                  # runs three jobs
    DIE=job-2 python examples/quickstart.py        # job-2 is SIGKILLed inside `transform`
    DIE=job-2 python examples/quickstart.py        # same command: job-1 and job-2's fetch are skipped

Delete quickstart.db* and the `died` marker to start over.
"""

import asyncio
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))  # run from a clone, no install

import stitch  # noqa: E402

HERE = Path(__file__).parent
stitch.configure(os.environ.get("QUICKSTART_DB", HERE / "quickstart.db"))
MARKER = Path(os.environ.get("QUICKSTART_MARKER", HERE / "died"))


@stitch.step
async def fetch(job):
    print(f"  fetch     {job['id']}", flush=True)
    await asyncio.sleep(0.3)
    return {"rows": 3}


@stitch.step
async def transform(job, data):
    print(f"  transform {job['id']}", flush=True)
    await asyncio.sleep(0.3)
    if os.environ.get("DIE") == job["id"] and not MARKER.exists():
        MARKER.write_text("died")
        os.kill(os.getpid(), 9)  # simulate an OOM-killed pod: no cleanup, no finally
    return {"rows": data["rows"] * 2}


@stitch.step
async def load(job, data):
    print(f"  load      {job['id']}", flush=True)
    await asyncio.sleep(0.3)
    return f"loaded {data['rows']} rows"


@stitch.workflow(key=lambda job: job["id"])  # the key identifies one unit of work
async def pipeline(job):
    data = await fetch(job)
    data = await transform(job, data)
    return await load(job, data)


async def main():
    for n in (1, 2, 3):
        print(f"job-{n}:", await pipeline({"id": f"job-{n}"}))


if __name__ == "__main__":
    asyncio.run(main())
