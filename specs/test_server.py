import sys

import pytest
from tqdm import trange

from zaku import TaskQ

task_queue = TaskQ(name="jq-debug", uri="http://localhost:9000")
task_queue.init_queue()


@pytest.mark.dependency(name="clear_queue")
def test_clear_queue():
    """Test adding and retrieving multiple tasks"""
    while True:
        with task_queue.pop() as task:
            if task is None:
                break
    print("cleared out the queue")


@pytest.mark.dependency(name="add_5_tasks", depends=["clear_queue"])
def test_add_5_tasks():
    """adding"""
    for i in trange(5, file=sys.stdout):
        task_queue.add({"step": i, "param_2": f"key-{i}"})


@pytest.mark.dependency(depends=["add_5_tasks", "clear_queue"])
def test_main():
    """Test adding and retrieving multiple tasks"""
    for i in range(10):
        with task_queue.pop() as task:
            if task is None:
                break

    assert i == 5, "should retrieve five task objects"


@pytest.mark.dependency(depends=["clear_queue"])
def test_numpy_tensor():
    """Test the ability to send and retrieve numpy arrays"""
    import numpy as np

    task = {"step": 0, "array": np.random.random([5, 10])}
    task_queue.add(task)
    with task_queue.pop() as retrieved_task:
        assert np.allclose(
            retrieved_task["array"], task["array"]
        ), "numpy arrays mismatch."


@pytest.mark.dependency(depends=["clear_queue"])
def test_torch_tensor():
    """Test the ability to send and retrieve pytorch tensors"""
    import torch

    task = {"step": 0, "tensor": torch.rand([5, 10])}
    task_queue.add(task)

    with task_queue.pop() as retrieved_task:
        assert torch.allclose(
            retrieved_task["tensor"], task["tensor"]
        ), "torch tensor mismatch."


@pytest.mark.dependency(depends=["clear_queue"])
def test_images():
    """Test the ability to send and retrieve pytorch tensors"""
    from pathlib import Path
    from PIL import Image
    import numpy as np

    img = Image.open(Path(__file__).parent / "assets/zaku.png")

    task = {"step": 0, "png": img}
    task_queue.add(task)

    with task_queue.pop() as retrieved_task:
        pass

    retrieved_img = np.array(retrieved_task["png"])
    original = np.array(img)

    np.allclose(original, retrieved_img)
