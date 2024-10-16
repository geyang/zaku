import sys

from zaku import TaskQ

task_queue = TaskQ(name="ZAKU_TEST:debug-queue")

import asyncio

i = 0
topic_id = "ge-debug-1"


async def publish():
    for i in range(5):
        result = task_queue.publish(
            {"step": i, "param_2": f"key-{i}", "very_large": " " * 8142, "haha": 0},
            topic=topic_id,
        )
        await asyncio.sleep(0.1)
        print("published", result)
        sys.stdout.flush()
        # return


async def main():
    await asyncio.gather(
        publish(),
    )


asyncio.run(main())

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
