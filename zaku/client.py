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

    Helper Methods
    +++++++++++++++++++++++

    .. automethod:: print_info

    Queue Info
    +++++++++++++++++++++++

    .. automethod:: count
    .. automethod:: __len__

    Task Life-cycle Methods
    +++++++++++++++++++++++

    .. automethod:: init_queue
    .. automethod:: add
    .. automethod:: take
    .. automethod:: mark_done
    .. automethod:: mark_reset
    .. automethod:: pop
    .. automethod:: clear_queue
    .. automethod:: house_keeping

    PubSub and RPC Methods
    +++++++++++++++++++++++

    .. automethod:: publish
    .. automethod:: subscribe_one
    .. automethod:: subscribe_stream
    .. automethod:: rpc
    .. automethod:: rpc_stream

    Async Workflow
    +++++++++++++++

    .. automethod:: gather
    .. automethod:: gather_one
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
        """Print the current configurations of the queue.

        Useful for debugging or when connection fails.
        """
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

        if self.verbose:
            print("creating queue...", self.name)
            self.print_info()

        # Establish clean error traces for better debugging.
        with suppress(requests.exceptions.ConnectionError):
            res = requests.put(self.uri + "/queues", json={"name": self.name})
            return res.status_code == 200, "failed"

        self.print_info()
        raise ConnectionError("Queue creation failed, check connection.").with_traceback(None)

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
            try:
                return int(res.text)
            except Exception:
                return None
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
        payload = Payload(**value)

        json = {
            "queue": self.name,
            # keep compatibility: if caller passes a key, we include it; otherwise server will generate
            **({"job_id": key} if key is not None else {}),
            "payload": payload.serialize(),
            # "ttl": self.ttl,
        }
        # ues msgpack to serialize the data. Bytes are the most efficient.
        res = requests.put(
            self.uri + "/tasks",
            msgpack.packb(json, use_bin_type=True),
        )
        if res.status_code == 200 and res.content:
            data = msgpack.loads(res.content)
            return data.get("job_id", key)
        raise Exception(f"Failed to add job to {self.uri}.", res.content)

    def count(self):
        """Count the number of available jobs in the queue.

        .. code-block:: python

            num_of_jobs = queue.count()

        :return None if the queue does not exist. This happens when there is no stale jobs.
                0 if the queue only contains stale jobs
                number if the queue contains open jobs (created)
        """
        response = requests.get(
            self.uri + "/tasks/counts",
            json={"queue": self.name},
        )

        if response.status_code != 200:
            raise RuntimeError(f"Failed to count job from {self.uri}.", response.content)

        elif not response.content:
            return None

        data = msgpack.loads(response.content)
        return data.get("counts", None)

    def __len__(self):
        """Returns the number of available jobs in the queue.

        .. code-block:: python

            length = len(queue)


        .. hint::

            This is an alias of TaskQ.count.

        :return None if the queue does not exist. This happens when there is no stale jobs.
                0 if the queue only contains stale jobs
                number if the queue contains open jobs (created)
        """
        return self.count()

    def take(self):
        """Grab a job that has not been grabbed from the queue."""
        response = requests.post(
            self.uri + "/tasks",
            json={"queue": self.name},
        )

        if response.status_code != 200:
            raise RuntimeError(f"Failed to grab job from {self.uri}.", response.content)

        elif not response.content:
            return None
            # raise RuntimeError("response from zaku server is empty")

        data = msgpack.loads(response.content)
        # print("take ==> ", data)
        payload = data.get("payload", None)
        return data["job_id"], Payload.deserialize(payload) if payload else None

    def mark_done(self, job_id):
        """Mark a job as done."""
        res = requests.delete(self.uri + "/tasks", json={"queue": self.name, "job_id": job_id})
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

    def unstale_tasks(self, ttl=300):
        """Remove all jobs in a queue. Useful when stale jobs degrades performance."""
        res = requests.put(
            self.uri + "/tasks/unstale",
            json={
                "queue": self.name,
                "ttl": ttl,  # the ttl is not used.
            },
        )
        if res.status_code == 200:
            return True

        raise Exception(f"Failed to do house keeping on {self.uri}.", res.content)

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

    def gather_one(self, job, gather_tokens=None, **kwargs):
        """Gather the jobs (not quite, will fix - Ge)

        Usage
        +++++

        .. code-block:: python

            queue = TaskQ()

            tokens = None
            for i in range(30):
                is_done, tokens = job_queue.gather_one(jobs, tokens)

            # this is blocking.
            if is_done():
                print("done")

        :param self:
        :type self: TaskQ
        :param jobs:
        :type jobs: dict
        :param gather_tokens: this is a singleton, a set that contains just one element, unless you pass in another token set.
        :type gather_tokens: Union[None, set]
        :param prefix:
        :type prefix: str
        :return: Union[Callable, set]
        :rtype:
        """
        return self.gather(jobs=[job], gather_tokens=gather_tokens, **kwargs)

    def gather(self, jobs: list, gather_tokens=None, prefix="{self.name}.return-queue"):
        """Gather the jobs (not quite, will fix - Ge)

        Usage
        +++++

        .. code-block:: python

            queue = TaskQ()

            jobs = [dict(seed=i) for i in range(30)]
            is_done, tokens = job_queue.gather(jobs)

            # this is blocking.
            if is_done():
                print("done")


        Now, to add jobs in batches:

        .. code-block:: python

            queue = TaskQ()

            jobs = [dict(seed=i) for i in range(30)]
            is_done, tokens = job_queue.gather(jobs)

            # add the second batch
            jobs = [dict(seed=i) for i in range(30, 50)]
            is_done, tokens = job_queue.gather(jobs, tokens)

            # now wait for done.
            if is_done():
                print("done")

        :param self:
        :type self: TaskQ
        :param jobs:
        :type jobs: dict
        :param gather_tokens: This is a set that you can add to, contains the list of tokens
        :type gather_tokens: Union[None, set]
        :param prefix:
        :type prefix: str
        :return: Union[Callable, set]
        :rtype:
        """
        from uuid import uuid4

        r_id = uuid4()
        r_queue_name = prefix.format(self=self, r_id=r_id)
        gather_queue = TaskQ(name=r_queue_name)

        gather_tokens = gather_tokens or set()

        for job in jobs:
            # this casts it to string
            gather_token = f"gather-{uuid4()}"
            gather_tokens.add(gather_token)

            r_spec = {
                "_gather_id": r_queue_name,
                "_gather_token": gather_token,
            }

            # # after the set should contain all
            # for job in jobs:
            self.add({**r_spec, **job})

        def is_done(blocking=False, sleep=0.1):
            import time

            nonlocal gather_queue, gather_tokens

            job = True
            while gather_tokens and (blocking or job):
                # this is not ideal
                ret = gather_queue.take()
                if ret is None:
                    job = None
                    time.sleep(sleep)
                    continue

                job_id, job = ret
                gather_queue.mark_done(job_id)

                try:
                    gt = job["_gather_token"]
                except KeyError:
                    gather_queue.clear_queue()
                    raise

                try:
                    gather_tokens.remove(gt)
                except KeyError:
                    pass
                # no sleep here.

            return not gather_tokens

        return is_done, gather_tokens
