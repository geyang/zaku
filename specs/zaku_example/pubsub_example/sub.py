from zaku import TaskQ

task_queue = TaskQ(name="ZAKU_TEST:debug-queue")

"""adding"""
import asyncio

i = 0
topic_id = "ge-debug-1"


async def subscribe():
    print('subscribing')
    result = task_queue.subscribe_one(topic_id, timeout=5)
    print(">>>", result.keys())


async def main():
    await asyncio.gather(
        subscribe(),
        subscribe(),
        subscribe(),
        subscribe(),
        subscribe(),
        subscribe(),
        subscribe(),
    )


asyncio.run(main())
