from contextlib import contextmanager
from uuid import uuid4

import msgpack
import requests
from params_proto import PrefixProto, Proto, Flag
from typing import Dict

from zaku.interfaces import Payload


class TaskQ(PrefixProto, cli=False):
    """TaskQ Client
    ----------------

    This is the client that interacts with the TaskQ server. We do not
    include the configs in the command line because the TaskServer is
    the primary entry point from the command line.

    We do provide the option to load environment variables for these
    configurations.

    Usage
    +++++

    .. code-block:: shell

        # Put this into an .env file (without the exports)
        export ZAKU_URI=http://localhost:9000
        export ZAKU_QUEUE_NAME=jq-debug-1

    Now you can create a queue like this:

    .. code-block:: python

        queue = TaskQ()

    ::

        Out[2]: {
            "uri": "http://localhost:9000",
            "name": "jq-debug-1",
            "ttl": 5.0
        }


    .. automethod:: init_queue
    .. automethod:: add
    .. automethod:: take
    .. automethod:: pop
    .. automethod:: mark_done
    .. automethod:: mark_reset
    """

    uri: str = Proto(
        "http://localhost:9000",
        env="ZAKU_URI",
        help="host end point, including protocol and port.",
    )
    """host endpoint uri, including protocol and port.
    
    .. code-block:: python
    
        uri: str = Proto(
            "http://localhost:9000",
            env="ZAKU_URI",
        )   
    """

    name: str = Proto(
        f"jq-{uuid4()}",
        env="ZAKU_QUEUE_NAME",
        help="""This is the name of the queue. It is unique to the client.""",
    )
    """This is the name of the queue. It is unique to the client. Defaults to a random uuid
    to avoid collision, but you usually want to supply a name so that it is easier to find
    the queue. Zaku also reads from environment variables to side-load the configurations.
    
    .. code-block:: python
    
        name: str = Proto(
            f"jq-{uuid4()}",
            env="ZAKU_QUEUE_NAME",
        )
    """

    ttl: float = Proto(5.0, "time to live in seconds. Defaults to 5.")
    """time to live in seconds. Defaults to 5."""
    no_init: bool = Flag("Flag for skipping the queue creation.")
    """Flag for skipping the queue creation."""

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
        res = requests.put(self.uri + "/queues", json={"name": self.name})
        return res.status_code == 200

    def add(self, value: Dict, *, key=None):
        """Append a job to the queue."""
        if key is None:
            key = str(uuid4())

        payload = Payload(**value)

        json = {
            "queue": self.name,
            "job_id": key,
            "payload": payload.serialize(),
            "ttl": self.ttl,
        }
        # ues msgpack to serialize the data. Bytes are the most efficient.
        res = requests.put(
            self.uri + "/jobs",
            msgpack.packb(json, use_bin_type=True),
        )
        if res.status_code == 200:
            return key
        raise Exception(f"Failed to add job to {self.uri}.")

    def take(self):
        """Grab a job that has not been grabbed from the queue."""
        response = requests.post(
            self.uri + "/jobs",
            json={"queue": self.name},
        )

        if response.status_code != 200:
            raise Exception(f"Failed to grab job from {self.uri}.")
        elif response.text == "EMPTY":
            return

        data = msgpack.loads(response.content)
        # print("take ==> ", data)
        payload = data.get("payload", None)
        return data["job_id"], Payload.deserialize(payload) if payload else None

    def mark_done(self, job_id):
        """Mark a job as done."""
        res = requests.delete(
            self.uri + "/jobs", json={"queue": self.name, "job_id": job_id}
        )
        if res.status_code == 200:
            return True
        raise Exception(f"Failed to mark job as done on {self.uri}.")

    def mark_reset(self, job_id):
        res = requests.post(
            self.uri + "/jobs/reset",
            json={"queue": self.name, "job_id": job_id, "op": "reset"},
        )
        if res.status_code == 200:
            return True
        raise Exception(f"Failed to reset job on {self.uri}.")

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
