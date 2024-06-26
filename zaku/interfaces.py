from io import BytesIO
from time import time, perf_counter
from types import SimpleNamespace
from typing import Literal, Any, Coroutine, Dict, Union, TYPE_CHECKING, Tuple

import msgpack
import numpy as np

if TYPE_CHECKING:
    import redis
    import torch

ZType = Literal["numpy.ndarray", "torch.Tensor", "generic"]


class ZData:
    @staticmethod
    def encode(data: Union["torch.Tensor", np.ndarray]):
        """This converts arrays and tensors to z-format."""
        import torch

        T = type(data)
        from PIL.Image import Image

        if isinstance(data, Image):
            # we always move to CPU
            with BytesIO() as buffer:
                # use the format of the Image object, default to PNG.
                data.save(buffer, format=data.format or "PNG")
                binary = buffer.getvalue()

            return dict(ztype="image", b=binary)
        elif T is np.ndarray:
            # need to support other numpy array types, including mask.
            binary = data.tobytes()
            return dict(
                ztype="numpy.ndarray",
                b=binary,
                dtype=str(data.dtype),
                shape=data.shape,
            )
        elif T is torch.Tensor:
            # we always move to CPU
            np_v = data.cpu().numpy()
            binary = np_v.tobytes()
            return dict(
                ztype="torch.Tensor",
                b=binary,
                dtype=str(np_v.dtype),
                shape=np_v.shape,
            )
        else:
            return data
            # return dict(ztype="generic", b=data)

    @staticmethod
    def get_ztype(data: Dict) -> Union[ZType, None]:
        """check if it is z-payload"""
        if type(data) is dict and "ztype" in data:
            return data["ztype"]

    @staticmethod
    def decode(zdata):
        import torch

        T = ZData.get_ztype(zdata)
        if not T:
            return zdata
        elif T == "image":
            from PIL import Image

            buff = BytesIO(zdata["b"])
            image = Image.open(buff)
            return image

        elif T == "numpy.ndarray":
            # need to support other numpy array types, including mask.
            array = np.frombuffer(zdata["b"], dtype=zdata["dtype"])
            array = array.reshape(zdata["shape"])
            return array
        elif T == "torch.Tensor":
            array = np.frombuffer(zdata["b"], dtype=zdata["dtype"])
            # we copy the array because the buffered version is non-writable.
            array = array.reshape(zdata["shape"]).copy()
            torch_array = torch.Tensor(array)
            return torch_array
        else:
            raise TypeError(f"ZData type {T} is not supported")


class Payload(SimpleNamespace):
    # class attributes are not serialized.
    greedy = True
    """Set to False to avoid greedy convertion, and make it go faster"""

    def __init__(self, _greedy=None, **payload):
        if _greedy:
            self.greedy = _greedy

        super().__init__(**payload)

    def serialize(self):
        payload = self.__dict__
        # we serialize components key value pairs
        if self.greedy:
            data = {k: ZData.encode(v) for k, v in payload.items()}
            data["_greedy"] = self.greedy
            msg = msgpack.packb(data, use_bin_type=True)
        else:
            msg = msgpack.packb(payload, use_bin_type=True)

        return msg

    @staticmethod
    def deserialize(payload) -> Dict:
        unpacked = msgpack.unpackb(payload, raw=False)
        is_greedy = unpacked.pop("_greedy", None)
        if not is_greedy:
            return unpacked
        else:
            data = {}

            for k, v in unpacked.items():
                data[k] = ZData.decode(v)

            return data

    @staticmethod
    def deserialize_unpacked(unpacked) -> Dict:
        """used with msgpack.Unpacker in the streaming mode, to let the unpacker
        handle the end of message. Handling it ourselves is messy.
        """
        is_greedy = unpacked.pop("_greedy", None)
        if not is_greedy:
            return unpacked
        else:
            data = {}

            for k, v in unpacked.items():
                data[k] = ZData.decode(v)

            return data


class Job(SimpleNamespace):
    created_ts: float
    status: Literal[None, "in_progress", "created"] = "created"
    grab_ts: float = None

    # value: Any = None
    # payload: bytes = None
    # """This is the binary encoding from the msgpack. """
    # ttl: float = None

    @staticmethod
    async def create_queue(r: Union["redis.asyncio.Redis", "redis.sentinel.asyncio.Redis"], name, *, prefix):
        from redis import ResponseError
        from redis.commands.search.field import TagField, NumericField
        from redis.commands.search.indexDefinition import IndexType, IndexDefinition

        index_name = f"{prefix}:{name}"
        index_prefix = f"{prefix}:{name}:"

        schema = (
            NumericField("$.created_ts", as_name="created_ts"),
            TagField("$.status", as_name="status"),
            NumericField("$.grab_ts", as_name="grab_ts"),
            # TextField("$.value", as_name="value"),
        )

        try:
            await r.ft(index_name).create_index(
                schema,
                definition=IndexDefinition(
                    prefix=[index_prefix],
                    index_type=IndexType.JSON,
                ),
            )
        except ResponseError as e:
            if "Index already exists" in str(e):
                return

    @staticmethod
    async def remove_queue(r: Union["redis.asyncio.Redis", "redis.sentinel.asyncio.Redis"], queue, *, prefix):
        index_name = f"{prefix}:{queue}"
        return await r.ft(index_name).dropindex()

    @staticmethod
    def add(
        r: Union["redis.asyncio.Redis", "redis.sentinel.asyncio.Redis"],
        queue: str,
        *,
        prefix: str,
        # value: Any,
        payload: bytes = None,
        job_id: str = None,
        # ttl: float = None,
    ) -> Coroutine:
        from uuid import uuid4

        job = Job(
            created_ts=time(),
            status="created",
            # value=value,
            # ttl=ttl,
        )
        if job_id is None:
            job_id = str(uuid4())

        entry_key = f"{prefix}:{queue}:{job_id}"

        p = r.pipeline()
        if payload:
            p.set(entry_key + ".payload", payload)
        p.json().set(entry_key, ".", vars(job))
        return p.execute(raise_on_error=False)

    @staticmethod
    async def count_files(r: Union["redis.asyncio.Redis", "redis.sentinel.asyncio.Redis"], queue, *, prefix) -> int:
        from redis.commands.search.query import Query
        from redis.commands.search.result import Result

        index_name = f"{prefix}:{queue}"

        # Create the query to count the files with the status 'created'
        q = Query("@status: { created }").paging(0, 0)  # Set paging to 0, 0 to avoid fetching any actual documents
        result: Result = await r.ft(index_name).search(q)

        return result.total  # Return the total number of matching documents

    @staticmethod
    async def take(r: Union["redis.asyncio.Redis", "redis.sentinel.asyncio.Redis"], queue, *, prefix) -> Tuple[Any, Any]:
        # Lua script for atomic operation

        lua_script = """
        local index_name = KEYS[1]
        local current_time = ARGV[1]

        -- Search for the job with status 'created'
        local job_result = redis.call('FT.SEARCH', index_name, '@status:{created}', 'LIMIT', '0', '1')
        if tonumber(job_result[1]) == 0 then
            return {nil, nil}
        end

        local job_id = job_result[2]
        redis.call('JSON.SET', job_id, '$.status', '"in_progress"')
        redis.call('JSON.SET', job_id, '$.grab_ts', current_time)
        return {job_id}
        """

        index_name = f"{prefix}:{queue}"

        # Execute Lua script
        current_time = str(time())
        result = await r.eval(lua_script, 1, index_name, current_time)

        if not result or result[0] is None:
            return None, None

        job_key = result[0].decode() if isinstance(result[0], bytes) else result[0]
        payload = await r.get(job_key + ".payload")  # Retrieve the payload separately as bytes
        job_id = job_key[len(index_name) + 1 :]

        return job_id, payload

    @staticmethod
    async def publish(
        r: Union["redis.asyncio.Redis", "redis.sentinel.asyncio.Redis"],
        queue: str,
        *,
        payload: bytes,
        topic_id: str,
        prefix: str,
    ) -> Coroutine:
        """Publish a job to a key --- this is not saved in the queue and is ephemeral."""
        # we don't use folders so that these do not get entangled with the job queue.
        entry_key = f"{prefix}:{queue}.topics:{topic_id}"
        subscribers = await r.publish(entry_key, payload)
        return subscribers

    # todo: implement streaming mode.
    @staticmethod
    async def subscribe(
        r: Union["redis.asyncio.Redis", "redis.sentinel.asyncio.Redis"],
        queue: str,
        *,
        topic_id: str,
        prefix: str,
        timeout: float = 0.1,
    ) -> str:
        """Returns the first non-empty message."""
        topic_name = f"{prefix}:{queue}.topics:{topic_id}"

        end_time = perf_counter() + timeout

        async with r.pubsub() as pb:
            await pb.subscribe(topic_name)

            while perf_counter() < end_time:
                message = await pb.get_message(
                    ignore_subscribe_messages=True,
                    timeout=min(0.1, timeout),
                )
                if message is None:
                    pass
                elif message["type"] == "message":
                    payload = message["data"]
                    return payload

    @staticmethod
    async def subscribe_stream(
        r: Union["redis.asyncio.Redis", "redis.sentinel.asyncio.Redis"],
        queue: str,
        *,
        topic_id: str,
        prefix: str,
        timeout: float = 0.1,
    ):
        topic_name = f"{prefix}:{queue}.topics:{topic_id}"

        end_time = perf_counter() + timeout

        async with r.pubsub() as pb:
            await pb.subscribe(topic_name)

            while perf_counter() < end_time:
                message = await pb.get_message(
                    ignore_subscribe_messages=True,
                    timeout=min(0.1, timeout),
                )
                if message is None:
                    pass
                elif message["type"] == "message":
                    payload = message["data"]
                    yield payload

    @staticmethod
    async def remove(r: Union["redis.asyncio.Redis", "redis.sentinel.asyncio.Redis"], job_id, queue, *, prefix):
        entry_name = f"{prefix}:{queue}:{job_id}"

        p = r.pipeline()

        if job_id == "*":
            count = 0
            async for key in r.scan_iter(entry_name):
                p = p.unlink(key)
                count += 1
            await p.execute(raise_on_error=False)
            return count

        # fmt: off
        response = p.unlink(entry_name) \
                    .unlink(entry_name + ".payload") \
                    .execute(raise_on_error=False)
        # fmt: on
        return await response

    @staticmethod
    def reset(r: Union["redis.asyncio.Redis", "redis.sentinel.asyncio.Redis"], job_id, queue, *, prefix):
        entry_name = f"{prefix}:{queue}:{job_id}"

        p = r.pipeline()
        p = p.json().set(entry_name, "$.status", "created")
        p = p.json().delete(entry_name, "$.grab_ts")

        return p.execute(raise_on_error=False)

    @staticmethod
    async def unstale_tasks(r: Union["redis.asyncio.Redis", "redis.sentinel.asyncio.Redis"], queue, *, prefix, ttl=None):
        from redis.commands.search.query import Query
        from redis.commands.search.result import Result

        index_name = f"{prefix}:{queue}"

        if ttl:
            q = Query(f"@status: {{ in_progress }} @grab_ts:[0 {time() - ttl}]")  # .paging(0, 1)
        else:
            q = Query("@status: { in_progress }")  # .paging(0, 1)

        result: Result = await r.ft(index_name).search(q)

        p = r.pipeline()
        for doc in result.docs:
            p = p.json().set(doc.id, "$.status", "created")
            p = p.json().delete(doc.id, "$.grab_ts")

        return await p.execute(raise_on_error=False)
