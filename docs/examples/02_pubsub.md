
# Zaku PubSub Example

This example shows you how to subscribe and publish to topics, that can be
used for broadcasting signals or data to multiple subscribers.

Published messages are ephemeral and are not stored in the queue. If you want
to store messages, you should use the TaskQ.

This API is used by the RPC api for getting results back from the workers.

```python
from zaku import TaskQ

task_queue = TaskQ()
```
```
{'uri': 'http://localhost:9000', 'name': 'jq-57276774-71c2-4925-8508-1f416c300b19', 'ttl': 5.0, 'no_init': None, 'verbose': None, 'ZAKU_USER': None, 'ZAKU_KEY': None}
```


## Example of a Publisher

To publish to a channel, simply call the `task_queue.publish` method. 

Run the following code snippet in a process.

```python
from multiprocessing import Process

def publish(topic_id="example-topic"):
    """A publisher function. We run this in a separate process to publish
    messages to the channel."""
    from time import sleep

    sleep(0.1)

    for i in range(5):
        n = task_queue.publish({"step": i, "param_2": f"key-{i}"}, topic=topic_id)
        sleep(0.1)
        print("published to ", n, "subscribers.")

p = Process(target=publish)
p.start()
```

## Simple Subscription

We can subscribe to a topic using the `task_queue.subscribe_one` method. This
takes the first message from the topic and returns it.

```python
result = task_queue.subscribe_one("example-topic", timeout=5)

print(">>>", result)
assert result["step"] == 0, "the step should be correct"
```

## Streaming Subscription

Sometimes we want to recieve a stream of messages from a topic. We can use the
`task_queue.subscribe_stream` method to do this. This function returns a generator
that we can iterate over.

Both APIs are synchronous.

```python
# remember to run the publisher after subscription!
stream = task_queue.subscribe_stream("example-topic", timeout=5)

for i, result in enumerate(stream):
    print(">>>", result)
    assert result["step"] == i, "the step should be correct"

assert i == 4, "there are 5 in total."
```
