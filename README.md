# JobQ, a fast Job Queue for ML Workloads

```
# Getting Started

To get a quick overview of what you can do with `jobq`, check out the following:

- take a look at the basic tutorial or the tutorial for robotics:
  - [JobQ Basics](tutorials/basics)
  - [Tutorial for Roboticists](tutorials/robotics)
- or try to take a look at the example gallery [here](examples/01_simple_queue)

Install jobq --- the latest version is `{VERSION}` on [pypi](https://pypi.org/project/jobq/{VERSION}/).

```python
pip install -U 'jobq=={VERSION}'
```

Supposed you have a JobServer running at `localhost:9000`.

**Adding Jobs**:

```python
from jobq import JobQ

queue = JobQ(name="my-test-queue", host="localhost", port=9000)

for i in range(100):
    queue.add_job({"job_id": i, "seed": i * 100})
```

**Retrieving Jobs**:

```python
from jobq import JobQ

queue = JobQ(name="my-test-queue", host="localhost", port=9000)

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

Now, we offer a context manager `JobQ.pop`, which automatically catches exceptions and resets the job (or marks it complete).

```python
from jobq import JobQ

queue = JobQ(name="my-test-queue", host="localhost", port=9000)

with queue.pop() as job:
  if job is None:
    print("No job available")
  
  print("Retrieved job:", job)
```

## Developing JobQ (Optional)

If you want to develop jobq, you can install it in editable mode plus dependencies
relevant for building the documentations:

```shell
cd jobq
pip install -e '.[all]'
```

To build the documentations, run

```shell
make docs
```

```

```
