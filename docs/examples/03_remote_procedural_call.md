
# Zaku Remote Procedural Call Example

This example shows you how to use a remote worker to execute a function, 
and collect its result. This is useful for offloading work to a separate
process, or a separate machine and getting the results back.

```python
from zaku import TaskQ

queue = TaskQ()
```

## Simple Remote Procedure Call (single return)

We can call a remote function using the `task_queue.rpc` method. 

```python
queue_name = "ZAKU_TEST:debug-rpc-queue"
rpc_queue = TaskQ(name=queue_name, uri="http://localhost:9000")

result = rpc_queue.rpc(seed=100, _timeout=5)
assert result["seed"] == 100, "the seed should be correct"
```


How do we implement the worker process? We can use the following code snippet:

Run this either as a stand along script, or in a separate process.

```python
from multiprocessing import Process

def worker_process(queue_name):
    from time import sleep

    queue = TaskQ(name=queue_name, uri="http://localhost:9000")

    job = None
    while not job:
        with queue.pop() as job:
            if job is None:
                pass

            # _request_id is the topic to respond to. We pop it out.
            topic = job.pop("_request_id")

            # we simulate a long-running job.
            sleep(1.0)

            # we return the result to the response topic.
            queue.publish(
                {"result": "good", **job},
                topic=topic,
            )

p = Process(target=worker_process, args=("ZAKU_TEST:debug-rpc-queue",))
p.start()
```

## Streaming Remote Procedure Call

Sometimes we want to recieve a stream of messages from a topic. We can use the

`task_queue.rpc_stream` method to do this. This function returns a generator
that we can iterate over.

```python
queue_name = "ZAKU_TEST:debug-rpc-queue"
rpc_queue = TaskQ(name=queue_name, uri="http://localhost:9000")

# remember to run the worker process after calling this.
stream = rpc_queue.rpc_stream(start=5, end=10, _timeout=5)

for i, result in enumerate(stream):
    print(">>>", result)
    assert result["value"] == i + 5, "the value should be correct"
```


We can implement the streamer worker via a small modification of the worker process
from above: just publish multiple results to the same topic.

```python
from multiprocessing import Process

def streamer_process(queue_name):
    from time import sleep

    queue = TaskQ(name=queue_name, uri="http://localhost:9000")

    job = None
    while not job:
        with queue.pop() as job:
            if job is None:
                pass

            topic = job.pop("_request_id")
            args = job.pop("_args")

            # we simulate a long-running job.
            sleep(1.0)

            # we return a sequence of results
            for i in range(job["start"], job["end"]):
                sleep(0.1)
                queue.publish({"value": i}, topic=topic)

p = Process(target=streamer_process, args=(queue_name,))
p.start()
```
