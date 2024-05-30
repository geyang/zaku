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


@pytest.mark.dependency(name="add_5_tasks", depends=["empty_queue"])
def test_add_5_tasks():
    """adding"""
    for i in trange(5, file=sys.stdout):
        task_queue.add({"step": i, "param_2": f"key-{i}"})


@pytest.mark.dependency(name="count", depends=["add_5_tasks", "empty_queue"])
def test_counts():
    """Test adding and retrieving multiple tasks"""
    counts = task_queue.count()

    assert counts == 5, "should retrieve five task objects"


@pytest.mark.dependency(name="take", depends=["add_5_tasks", "count", "empty_queue"])
def test_take():
    """Test adding and retrieving multiple tasks"""
    for i in range(10):
        with task_queue.pop() as task:
            if task is None:
                break

    assert i == 5, "should retrieve five task objects"


@pytest.mark.dependency(depends=["empty_queue"])
def test_clear_queue():
    """Test adding and retrieving multiple tasks"""

    for i in trange(5, file=sys.stdout):
        task_queue.add({"step": i, "param_2": f"key-{i}"})

    task_queue.clear_queue()

    for i in range(10):
        with task_queue.pop() as task:
            if task is None:
                break

    assert i == 0, "should retrieve zero task objects after clearance"


@pytest.mark.dependency(depends=["empty_queue"])
def test_numpy_tensor():
    """Test the ability to send and retrieve numpy arrays"""
    import numpy as np

    task = {"step": 0, "array": np.random.random([5, 10])}
    task_queue.add(task)
    with task_queue.pop() as retrieved_task:
        assert np.allclose(retrieved_task["array"], task["array"]), "numpy arrays mismatch."


@pytest.mark.dependency(depends=["empty_queue"])
def test_torch_tensor():
    """Test the ability to send and retrieve pytorch tensors"""
    import torch

    task = {"step": 0, "tensor": torch.rand([5, 10])}
    task_queue.add(task)

    with task_queue.pop() as retrieved_task:
        assert torch.allclose(retrieved_task["tensor"], task["tensor"]), "torch tensor mismatch."


@pytest.mark.dependency(depends=["empty_queue"])
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
