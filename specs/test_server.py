import sys
from pprint import pformat

import pytest
from tqdm import trange

from zaku import TaskQ

job_queue = TaskQ(name="jq-debug", uri="http://localhost:9000")
job_queue.init_queue()


@pytest.mark.dependency()
def test_add():
    for i in trange(5, file=sys.stdout):
        job_queue.add({"step": i, "param_2": f"key-{i}"})


@pytest.mark.dependency(depends=["test_add"])
def test_main():
    """

    :return:
    :rtype:
    """
    print("")
    while True:
        with job_queue.pop() as job:
            if job is None:
                break
        print(f"job<{pformat(job)}>.")
