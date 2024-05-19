import sys

import pytest
from tqdm import trange

from zaku import TaskQ

task_queue = TaskQ(name="ZAKU_TEST:debug-queue", uri="http://localhost:9000")
task_queue.init_queue()


@pytest.mark.dependency(name="empty_queue")
def test_empty_queue():
    """Test adding and retrieving multiple tasks"""


    while True:
        with task_queue.pop() as task:
            if task is None:
                break
    print("cleared out the queue")


@pytest.mark.dependency(name="add_100_tasks", depends=["empty_queue"])
def test_add_100_tasks():
    """adding"""

    task_queue.clear_queue()

    for i in trange(2, file=sys.stdout):

        task_queue.add({"step": i, "param_2": f"key-{i}"})


@pytest.mark.dependency(name="take", depends=["add_100_tasks", "empty_queue"])
def test_unstale():
    """Test adding and retrieving multiple tasks"""
    import time

    task_queue.clear_queue()

    for i in trange(2, file=sys.stdout):

        task_queue.add({"step": i, "param_2": f"key-{i}"})

    for i in range(2):
        job_container = task_queue.take()
        print(job_container)

    time.sleep(1.0)

    job_container = task_queue.take()
    assert job_container is None, "should retrieve no task objects at this point."

    task_queue.unstale_tasks(ttl=0.1)

    for i in range(2):
        job_container = task_queue.take()
        assert job_container is not None, "should retrieve 100 task objects at this point."

