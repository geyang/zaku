from contextlib import contextmanager
from uuid import uuid4

import msgpack
import requests
# from requests_futures import sessions
from params_proto import PrefixProto, Proto, Flag


class JobQ(PrefixProto):
    host: str = Proto(
        "http://localhost:9000",
        help="host end point, including protocol and port.",
    )

    name = Proto(
        f"jq-{uuid4()}",
        help="""This is the name of the queue. It is unique to the client.""",
    )
    ttl = Proto(5, help="time to live. Defaults to 5.")
    no_init = Flag("Flag for skipping the queue creation.")

    def __post_init__(self):
        if not self.no_init:
            self.init_queue()

    def init_queue(self, name=None):
        """Create a new collection.

        :param name: (optional) The name of the queue.
        """
        if name:
            self.name = name

        print("creating queue:", self.name)
        res = requests.put(self.host + "/queues", json={"name": self.name})
        return res.status_code == 200

    def add(self, value, *, key=None):
        """Append a job to the queue."""
        if key is None:
            key = str(uuid4())

        json = {
            "queue": self.name,
            "job_id": key,
            "payload": msgpack.packb(value, use_bin_type=True),
            "ttl": self.ttl,
        }
        # ues msgpack to serialize the data. Bytes are the most efficient.
        res = requests.put(
            self.host + "/jobs",
            msgpack.packb(json, use_bin_type=True),
        )
        if res.status_code == 200:
            return key
        raise Exception(f"Failed to add job to {self.host}.")

    def take(self):
        """Grab a job that has not been grabbed from the queue."""
        response = requests.post(
            self.host + "/jobs",
            json={"queue": self.name},
        )

        if response.status_code != 200:
            raise Exception(f"Failed to grab job from {self.host}.")
        elif response.text == "EMPTY":
            return

        data = msgpack.loads(response.content)
        # print("take ==> ", data)
        payload = data.get("payload", None)
        return data["job_id"], msgpack.unpackb(payload) if payload else None

    def mark_done(self, job_id):
        """Mark a job as done."""
        res = requests.delete(
            self.host + "/jobs", json={"queue": self.name, "job_id": job_id}
        )
        if res.status_code == 200:
            return True
        raise Exception(f"Failed to mark job as done on {self.host}.")

    def mark_reset(self, job_id):
        res = requests.post(
            self.host + "/jobs/reset",
            json={"queue": self.name, "job_id": job_id, "op": "reset"},
        )
        if res.status_code == 200:
            return True
        raise Exception(f"Failed to reset job on {self.host}.")

    @contextmanager
    def pop(self):
        """Pop a job from the queue."""
        job_tuple = self.take()
        if not job_tuple:
            yield None
            return

        job_id, job = job_tuple
        try:
            yield job
        except Exception as e:
            self.mark_reset(job_id)
            raise e

        self.mark_done(job_id)
