from time import time
from uuid import uuid4

class JobQueue(dict):
    def __init__(self, ttl=5):
        """A simple job queue.

        Args:
            data (dict): a dictionary of jobs.
            ttl (int, optional): time to live. Defaults to 5.
        """
        super().__init__()
        self._ttl = ttl

    def take(self):
        """Grab a job that has not been grabbed from the queue."""

        for k in sorted(self.keys()):
            job = self[k]
            if job["status"] is None:
                continue
            job["grab_ts"] = time()
            job["status"] = "in_progress"
            break

        return job, lambda: self.mark_done(k), lambda: self.mark_reset(k)

    def append(self, job_params):
        """Append a job to the queue."""

        k = str(uuid4())
        self[k] = {
            "created_ts": time(),
            "status": None,
            "grab_ts": None,
            "job_params": job_params,
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