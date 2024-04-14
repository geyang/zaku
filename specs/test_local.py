from asyncio import sleep
from pprint import pformat

import pytest

from zaku.job_queue import SimpleJobQueue

job_queue = SimpleJobQueue()

for i in range(5):
    job_queue.add({"step": i, "param_2": f"key-{i}"})


@pytest.mark.asyncio
async def test_main():
    while job_queue:
        with job_queue.pop() as job:
            print(f"\nI took job\n{pformat(job)}.")
            await sleep(0.02)
