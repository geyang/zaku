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


def test_numpy_tensor():
    import numpy as np

    job = {"step": 0, "array": np.random.random([5, 10])}
    job_queue.add(job)
    with job_queue.pop() as retrieved_job:
        assert np.allclose(
            retrieved_job["array"], job["array"]
        ), "numpy arrays mismatch"


def test_torch_tensor():
    import torch

    job = {"step": 0, "tensor": torch.rand([5, 10])}
    job_queue.add(job)

    with job_queue.pop() as retrieved_job:
        assert torch.allclose(retrieved_job["tensor"], job["tensor"])
