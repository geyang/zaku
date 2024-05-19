from contextlib import contextmanager, suppress
from typing import Dict
from uuid import uuid4

import msgpack
import requests
from params_proto import PrefixProto, Proto, Flag

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
        export ZAKU_QUEUE_NAME=ZAKU_TEST:debug-queue-1

    Now you can create a queue like this:

    .. code-block:: python

        queue = TaskQ()

    ::

        Out[2]: {
            "uri": "http://localhost:9000",
            "name": "ZAKU_TEST:debug-queue-1",
            "ttl": 5.0
        }

    Task Queue Configurations
    +++++++++++++++++++++++++

    .. autoattribute:: uri
    .. autoattribute:: name
    .. autoattribute:: ttl
    .. autoattribute:: no_init

    Task Life-cycle Methods
    +++++++++++++++++++++++

    .. automethod:: init_queue
    .. automethod:: add
    .. automethod:: take
    .. automethod:: mark_done
    .. automethod:: mark_reset
    .. automethod:: pop
    .. automethod:: clear_queue

    PubSub and RPC Methods
    +++++++++++++++++++++++

    .. automethod:: publish
    .. automethod:: subscribe_one
    .. automethod:: subscribe_stream
    .. automethod:: rpc
    .. automethod:: rpc_stream

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

    verbose: bool = Flag("Flag printing the state of the queue")

    ZAKU_USER = Proto(env="ZAKU_USER", help="The user name for the queue.")
    ZAKU_KEY = Proto(env="ZAKU_KEY", help="The user name for the queue.")

    def __post_init__(self):
        if not self.no_init:
            self.init_queue()

    def print_info(self):
        print("=============================")
        for k, v in vars(self).items():
            print(f" {k} = {v}")
        print("=============================")

    def init_queue(self, name=None):
        """Create a new collection.

        :param name: (optional) The name of the queue.
        """
        if name:
            self.name = name

        print("creating queue...", self.name)

        if self.verbose:
            self.print_info()

        # Establish clean error traces for better debugging.
        with suppress(requests.exceptions.ConnectionError):
            res = requests.put(self.uri + "/queues", json={"name": self.name})
            return res.status_code == 200, "failed"

        self.print_info()
        raise ConnectionError(
            "Queue creation failed, check connection."
        ).with_traceback(None)

    def publish(self, value: Dict, *, topic=None):
        """Append a job to the queue."""
        if topic is None:
            topic = str(uuid4())

        payload = Payload(**value)

        json = {
            "queue": self.name,
            "topic_id": topic,
            "payload": payload.serialize(),
            # published messages are ephemeral.
            # "ttl": self.ttl,
        }
        # ues msgpack to serialize the data. Bytes are the most efficient.
        res = requests.put(
            self.uri + "/publish",
            msgpack.packb(json, use_bin_type=True),
        )
        if res.status_code == 200:
            return res.json()
        raise Exception(f"Failed to add job to {self.uri}.", res.content)

    def subscribe_one(self, topic: str, timeout=0.1):
        """subscribe to wait for one publishing event"""
        response = requests.post(
            self.uri + "/subscribe_one",
            json={"queue": self.name, "topic_id": topic, "timeout": timeout},
        )

        # todo: add timeout handling
        if response.status_code != 200:
            raise Exception(f"Failed to grab job from {self.uri}.", response.content)

        if not response.content:
            return

        # subscription does not have a job container.
        return Payload.deserialize(response.content)

    def subscribe_stream(self, topic: str, timeout=0.1):
        """subscribe to collect all publishing events"""
        delimiter = b"\n"
        response = requests.post(
            self.uri + "/subscribe_stream",
            json={"queue": self.name, "topic_id": topic, "timeout": timeout},
            stream=True,
        )
        response.raise_for_status()  # Raise an exception for HTTP errors
        unpacker = msgpack.Unpacker()

        for chunk in response.iter_content(chunk_size=8192):
            unpacker.feed(chunk)
            for unpacked in unpacker:
                yield Payload.deserialize_unpacked(unpacked)

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
            self.uri + "/tasks",
            msgpack.packb(json, use_bin_type=True),
        )
        if res.status_code == 200:
            return key
        raise Exception(f"Failed to add job to {self.uri}.", res.content)

    def take(self):
        """Grab a job that has not been grabbed from the queue."""
        response = requests.post(
            self.uri + "/tasks",
            json={"queue": self.name},
        )

        if response.status_code != 200:
            raise Exception(f"Failed to grab job from {self.uri}.", response.content)

        elif not response.content:
            return

        data = msgpack.loads(response.content)
        # print("take ==> ", data)
        payload = data.get("payload", None)
        return data["job_id"], Payload.deserialize(payload) if payload else None

    def mark_done(self, job_id):
        """Mark a job as done."""
        res = requests.delete(
            self.uri + "/tasks", json={"queue": self.name, "job_id": job_id}
        )
        if res.status_code == 200:
            return True
        raise Exception(f"Failed to mark job as done on {self.uri}.", res.content)

    def mark_reset(self, job_id):
        res = requests.post(
            self.uri + "/tasks/reset",
            json={"queue": self.name, "job_id": job_id},
        )
        if res.status_code == 200:
            return True
        raise Exception(f"Failed to reset job on {self.uri}.", res.content)

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
        except SystemExit as e:
            self.mark_done(job_id)
            raise e
        except Exception as e:
            self.mark_reset(job_id)
            raise e

        self.mark_done(job_id)

    def clear_queue(self):
        """Remove all jobs in a queue. Useful when stale jobs degrades performance."""
        res = requests.delete(
            self.uri + "/tasks",
            json={"queue": self.name, "job_id": "*"},
        )
        if res.status_code == 200:
            return True
        raise Exception(f"Failed to reset job on {self.uri}.", res.content)

    def rpc(self, *args, _timeout=1.0, **kwargs):
        """
        This function is a synchronous RPC function that is used to
        send rendering requests to the rendering server and collect
        the response. This is a blocking function that waits for the
        response from the worker before returning.

        Args:
            response_topic: The pubsub topic to return the response to
            args: The positional arguments
            kwargs: The keyword arguments

        :return:
        """

        from uuid import uuid4

        request_id = uuid4()

        topic_name = f"rpc-{request_id}"

        # push the request to the queue
        self.add(
            {
                "_request_id": topic_name,
                "_args": args,
                **kwargs,
            }
        )

        return self.subscribe_one(topic_name, timeout=_timeout)

    def rpc_stream(
        self,
        *args,
        _timeout=1.0,
        **kwargs,
    ):
        """
        This function is a synchronous RPC function that is used to
        send rendering requests to the rendering server and collect
        the response. This is a blocking function that waits for the
        response from the worker before returning.

        Args:
            response_topic: The pubsub topic to return the response to
            args: The positional arguments
            kwargs: The keyword arguments

        :return:
        """

        from uuid import uuid4

        request_id = uuid4()

        topic_name = f"rpc-{request_id}"

        # push the request to the queue
        self.add(
            {
                "_request_id": topic_name,
                "_args": args,
                **kwargs,
            }
        )

        return self.subscribe_stream(topic_name, timeout=_timeout)
