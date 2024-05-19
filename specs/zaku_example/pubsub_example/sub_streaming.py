from zaku import TaskQ

task_queue = TaskQ(name="ZAKU_TEST:debug-queue", uri="http://localhost:9000")

"""adding"""
import asyncio

i = 0
topic_id = "ge-debug-1"


async def subscribe_to_stream():
    print("subscribing")
    for data in task_queue.subscribe_stream(topic_id, timeout=10):
        print(">>>", data.keys())


async def main():
    await asyncio.gather(
        subscribe_to_stream(),
        # subscribe_to_stream(),
        # subscribe_to_stream(),
        # subscribe_to_stream(),
        # subscribe_to_stream(),
        # subscribe_to_stream(),
        # subscribe_to_stream(),
    )


asyncio.run(main())
