import sys

import pytest

from zaku import TaskQ

task_queue = TaskQ(name="ZAKU_TEST:debug-queue")
task_queue.init_queue()


def publish(topic_id):
    """A publisher function. We run this in a separate process to publish
    messages to the channel."""
    from time import sleep

    sleep(0.1)

    for i in range(5):
        n = task_queue.publish({"step": i, "param_2": f"key-{i}"}, topic=topic_id)
        sleep(0.1)
        print("published", n)
        sys.stdout.flush()


@pytest.mark.dependency(name="pubsub")
def test_pubsub():
    """Test the pubsub api."""

    from multiprocessing import Process

    topic_id = "ZAKU-TEST:test_pubsub-topic"

    p = Process(target=publish, args=(topic_id,))
    p.start()

    result = task_queue.subscribe_one(topic_id, timeout=5)
    print(">>>", result)
    assert result["step"] == 0, "the step should be correct"

    p.join()


@pytest.mark.dependency(name="pubsub_streaming")
def test_pubsub_streaming():
    """adding"""

    from multiprocessing import Process

    topic_id = "ZAKU-TEST:test_pubsub_streaming-topic"

    p = Process(target=publish, args=(topic_id,))
    p.start()

    stream = task_queue.subscribe_stream(topic_id, timeout=5)

    for i, result in enumerate(stream):
        print(">>>", result)
        assert result["step"] == i, "the step should be correct"

    assert i == 4, "there are 5 in total."

    p.join()


def worker_process(queue_name):
    from time import sleep

    queue = TaskQ(name=queue_name)

    job = None
    while not job:
        with queue.pop() as job:

            if job is None:
                continue

            topic = job.pop("_request_id")

            # we simulate a long-running job. Make sure you clear the queue first though.
            sleep(0.1)

            # we return the result to the response topic.
            queue.publish(
                {"result": "good", **job},
                topic=topic,
            )


@pytest.mark.dependency(name="test_rpc")
def test_rpc():
    """adding"""
    from time import sleep
    from multiprocessing import Process

    queue_name = "ZAKU_TEST:debug-rpc-queue"
    rpc_queue = TaskQ(name=queue_name)

    # this is important, otherwise the worker will get suck with
    # an old message.
    rpc_queue.clear_queue()

    p = Process(target=worker_process, args=(queue_name,))
    p.start()

    sleep(0.5)

    result = rpc_queue.rpc(seed=100, _timeout=2)
    assert result["seed"] == 100, "the seed should be correct"
    p.join()


def streamer_process(queue_name):
    from time import sleep

    queue = TaskQ(name=queue_name)

    job = None
    while not job:
        with queue.pop() as job:
            if job is None:
                continue

            topic = job.pop("_request_id")
            args = job.pop("_args")

            # we simulate a long-running job.
            sleep(1.0)

            # we return a sequence of results
            for i in range(job["start"], job["end"]):
                sleep(0.1)
                queue.publish({"value": i}, topic=topic)


@pytest.mark.dependency(name="test_rpc_streaming")
def test_rpc_streaming():
    """adding"""

    from multiprocessing import Process

    queue_name = "ZAKU_TEST:debug-rpc-queue"
    rpc_queue = TaskQ(name=queue_name)
    rpc_queue.init_queue()

    p = Process(target=streamer_process, args=(queue_name,))
    p.start()

    stream = rpc_queue.rpc_stream(start=5, end=10, _timeout=5)
    for i, result in enumerate(stream):
        print(">>>", result)
        assert result["value"] == i + 5, "the value should be correct"

    p.join()
