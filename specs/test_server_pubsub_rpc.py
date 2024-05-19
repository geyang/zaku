import pytest

from zaku import TaskQ

task_queue = TaskQ(name="jq-debug", uri="http://localhost:9000")
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
def test_pubsub():
    """adding"""

    i = 0
    result = task_queue.publish({"step": i, "param_2": f"key-{i}"}, topic="jq-debug")
    print("publication results in", result)


# @pytest.mark.dependency(name="add_5_tasks", depends=["empty_queue"])
# def test_rpc_long_polling():
#     """adding"""
#     import asyncio
#
#     def slow_task_handler():
#
#         while True:
#             with task_queue.pop() as task:
#                 if task is None:
#                     break
#
#                 print("task received", task)
#                 asyncio.sleep(1.0)
#                 task_queue.send("response-uuid", {"response": "success", "task": task})
#
#     def rpc_call_example():
#
#         i = 0
#         result = task_queue.rpc({"step": i, "param_2": f"key-{i}"})
#         print("we get results", result)
