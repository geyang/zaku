from time import time
from types import SimpleNamespace
from typing import Literal, Any, Tuple, Coroutine, Dict, Union

import msgpack
import numpy as np
import redis
from redis import ResponseError
from redis.commands.search.query import Query
from redis.commands.search.result import Result

ZType = Literal["numpy.ndarray", "torch.Tensor", "generic"]


class ZData:
    # data_types = {
    #     "numpy.ndarray":
    # }

    @staticmethod
    def encode(data: Union["torch.Tensor", np.ndarray]):
        """This converts arrays and tensors to z-format."""
        import torch

        T = type(data)
        if T is np.ndarray:
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
            data
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
        elif T == "numpy.ndarray":
            # need to support other numpy array types, including mask.
            array = np.frombuffer(zdata["b"], dtype=zdata["dtype"])
            array = array.reshape(zdata["shape"])
            return array
        elif T == "torch.Tensor":
            array = np.frombuffer(zdata["b"], dtype=zdata["dtype"])
            array = array.reshape(zdata["shape"])
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


class Job(SimpleNamespace):
    created_ts: float
    status: Literal[None, "in_progress", "created"] = "created"
    grab_ts: float = None
    # value: Any = None
    # payload: bytes = None
    # """This is the binary encoding from the msgpack. """
    ttl: float = None

    @staticmethod
    async def create_queue(r: redis.asyncio.Redis, name, *, prefix, smart=True):
        from redis.commands.search.field import TagField, NumericField
        from redis.commands.search.indexDefinition import IndexType, IndexDefinition

        index_name = f"{prefix}:{name}"
        index_prefix = f"{prefix}:{name}:"

        print("creating queue:", index_name)

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
        except ResponseError:
            if not smart:
                return

            await r.ft(index_name).dropindex()
            await r.ft(index_name).create_index(
                schema,
                definition=IndexDefinition(
                    prefix=[index_prefix],
                    index_type=IndexType.JSON,
                ),
            )

    @staticmethod
    async def remove_queue(r: redis.asyncio.Redis, queue, *, prefix):
        index_name = f"{prefix}:{queue}"
        return await r.ft(index_name).dropindex()

    @staticmethod
    def add(
        r: redis.asyncio.Redis,
        queue: str,
        *,
        prefix: str,
        # value: Any,
        payload: bytes = None,
        job_id: str = None,
        ttl: float = None,
    ) -> Coroutine:
        from uuid import uuid4

        job = Job(
            created_ts=time(),
            status="created",
            # value=value,
            ttl=ttl,
        )
        if job_id is None:
            job_id = str(uuid4())

        entry_key = f"{prefix}:{queue}:{job_id}"

        p = r.pipeline()
        p.json().set(entry_key, ".", vars(job))
        if payload:
            p.set(entry_key + ".payload", payload)
        return p.execute()

    @staticmethod
    async def take(r: redis.asyncio.Redis, queue, *, prefix) -> Tuple[str, Any]:
        index_name = f"{prefix}:{queue}"

        # note: search ranks results via FTIDF. Use aggregation to sort by created_ts
        q = Query("@status: { created }").paging(0, 1)
        result: Result = await r.ft(index_name).search(q)

        if not result.total:
            return None, None

        job = result.docs[0]
        p = r.pipeline()
        payload, *_ = (
            await p.get(job.id + ".payload")
            .json()
            .set(job.id, "$.status", "in_progress")
            .json()
            .set(job.id, "$.grab_ts", time())
            .execute()
        )

        job_id = job.id[len(index_name) + 1 :]
        return job_id, payload

    @staticmethod
    def remove(r: redis.asyncio.Redis, job_id, queue, *, prefix) -> Coroutine:
        entry_name = f"{prefix}:{queue}:{job_id}"

        p = r.pipeline()
        response = p.json().delete(entry_name).delete(entry_name + ".payload").execute()
        return response

    @staticmethod
    def reset_stale(r: redis.asyncio.Redis, queue, *, prefix, ttl=None):
        index_name = f"{prefix}:{queue}"

        if ttl:
            result = r.ft(index_name).search(
                "@status: { in_progress } @grab_ts: < {time() - ttl}"
            )
        else:
            result = r.ft(index_name).search("@status: { in_progress }")

        p = r.pipeline()
        for doc in result.docs:
            p = p.json().set(doc.id, "$.status", "created")
            p = p.json().delete(doc.id, "$.grab_ts", None)

        return p.execute()
