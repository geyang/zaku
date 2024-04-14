from time import time
from types import SimpleNamespace
from typing import Literal, Any, Tuple, Coroutine

import msgpack
import redis
from redis import ResponseError
from redis.commands.search.query import Query
from redis.commands.search.result import Result


# class Message(NamedTuple):
#     op: Literal["add", "take", "delete", "reset"]
#     queue: str
#     key: str
#     value: Any = None


class Job(SimpleNamespace):
    created_ts: float
    status: Literal[None, "in_progress", "created"] = "created"
    grab_ts: float = None
    # value: Any = None
    # payload: bytes = None
    # """This is the binary encoding from the msgpack. """
    ttl: float = None

    def serialize(self):
        msg = msgpack.packb(vars(self), use_bin_type=True)
        return msg

    @staticmethod
    async def deserialize(payload) -> "Job":
        data = msgpack.unpackb(payload, raw=False)
        return Job(**data)

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
        job_id = job_id or str(uuid4())

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
