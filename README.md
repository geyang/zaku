# Zaku, a fast Task Queue for ML Workloads

To get a quick overview of what you can do with `zaku`, check out the following:

- take a look at the basic tutorial or the tutorial for robotics:
  - [Zaku Basics](tutorials/basics)
- or try to take a look at the example gallery [here](examples/01_simple_queue)

Install zaku --- the latest version is `{VERSION}` on [pypi](https://pypi.org/project/zaku/{VERSION}/).

```python
pip install -U 'zaku[all]=={VERSION}'
```

**Setting Up Zaku Server**

The server script is installed as the command `zaku`. First, take a look at its options by running

```shell
zaku -h
```

This should show you the documents on all of the arguments. Now, to setup a zaku task queue server, run the following: This enables access from other than localhost.

````shell
zaku --host 0.0.0.0 --port 9000
````



**Adding Jobs**:

Supposed you have a TaskServer running at `localhost:9000`.

```python
from zaku import TaskQ

queue = TaskQ(name="my-test-queue", uri="http://localhost:9000")

for i in range(100):
    queue.add_job({"job_id": i, "seed": i * 100})
```

**Retrieving Jobs**:

```python
from zaku import TaskQ

queue = TaskQ(name="my-test-queue", uri="http://localhost:9000")

job_id, job = queue.take()
```

Now, after you have finished the job, you need to mark the job for completion. The way we do so is by calling

```python
queue.mark_done(job_id)
```

Sometimes when you worker responsible for completeing the job encounters a failure, you need to also put the job back into the queue so that other workers can retry. You can do so by calling

```python
queue.mark_reset()
```

Now, we offer a context manager `TaskQ.pop`, which automatically catches exceptions and resets the job (or marks it complete).

```python
from zaku import TaskQ

queue = TaskQ(name="my-test-queue", uri="http://localhost:9000")

with queue.pop() as job:
  if job is None:
    print("No job available")
  
  print("Retrieved job:", job)
```

## Developing Zaku (Optional)

If you want to develop zaku, you can install it in editable mode plus dependencies
relevant for building the documentations:

```shell
cd zaku
pip install -e '.[dev]'
```

To build the documentations, run

```shell
make docs
```
