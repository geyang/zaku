from contextlib import contextmanager
from time import time
from uuid import uuid4


class SimpleTaskQueue(dict):
    def __init__(self, ttl=5):
        """A simple job queue.

        Args:
            ttl (int, optional): time to live. Defaults to 5.
        """
        super().__init__()
        self._ttl = ttl

    def take(self):
        """Grab a job that has not been grabbed from the queue."""

        for k in self.keys():
            job = self[k]
            if job["status"] is None:
                continue
            job["grab_ts"] = time()
            job["status"] = "in_progress"
            break

        return job, lambda: self.mark_done(k), lambda: self.mark_reset(k)

    # def __bool__(self):
    #     pass

    @contextmanager
    def pop(self, ord=0):
        """Pop a job from the queue."""
        job, mark_done, mark_reset = self.take()
        try:
            yield job
        except Exception as e:
            mark_reset()
            raise e

        mark_done()

    def add(self, value, key=None):
        """Append a job to the queue."""
        k = key or str(uuid4())
        self[k] = {
            "created_ts": time(),
            "status": None,
            "grab_ts": None,
            "value": value,
        }

    def mark_done(self, key):
        """Mark a job as done."""
        del self[key]

    def mark_reset(self, key):
        self[key]["status"] = None

    def house_keeping(self):
        """Reset jobs that have become stale."""
        for job in self.values():
            if job["status"]:
                continue
            if job["grab_ts"] < (time() - self._ttl):
                job["status"] = None

