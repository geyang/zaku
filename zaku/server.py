import msgpack
import redis
from aiohttp import web
from params_proto import Proto, ParamsProto

from zaku.base import Server
from zaku.interfaces import Job

DEFAULT_PORT = 9000


class Redis(ParamsProto, prefix="redis", cli_parse=False):
    host = Proto(env="REDIS_HOST", default="localhost")
    port = Proto(env="REDIS_PORT", default=6379)
    password = Proto(env="REDIS_PASSWORD")
    db = Proto(env="REDIS_DB", default=0)


class TaskServer(ParamsProto, Server):
    """TaskServer

    This is the server that maintains the Task Queue.

    Usage::

        app = TaskServer()
        app.run()

    Arguments::

        port: int = 8012
            The port to run the server on.
        free_port: bool = True
            If True, kill the port before starting the server.
        static_root: str = "."
            The root directory to serve static files from.
        cors: str = "https://vuer.ai,https://dash.ml,http://localhost:8000,http://
            The CORS policy to use. Defaults to allow all.
        cert: str = None
            The path to the SSL certificate.

    .. automethod:: run
    """

    prefix = "Zaku-task-queues"

    # Queue Parameters
    queue_len = 100  # use a max length to avoid the memory from blowing up.

    # Server Parameters
    port = DEFAULT_PORT
    free_port = True
    static_root = "."
    cors = (
        "https://vuer.ai,https://dash.ml,http://localhost:8000,http://127.0.0.1:8000,*"
    )

    # SSL Parameters
    cert = Proto(None, dtype=str, help="the path to the SSL certificate")
    key = Proto(None, dtype=str, help="the path to the SSL key")
    ca_cert = Proto(None, dtype=str, help="the trusted root CA certificates")

    def __post_init__(self):
        Server.__post_init__(self)

        self.redis = redis.asyncio.Redis(**vars(Redis))

    async def create_queue(self, request: web.Request):
        data = await request.json()
        # print("==>", data)
        try:
            await Job.create_queue(self.redis, **data, prefix=self.prefix)
        except Exception:
            return web.Response(text="index already exists", status=400)

        return web.Response(text="OK")

    async def add_job(self, request: web.Request):
        msg = await request.read()
        data = msgpack.unpackb(msg)
        # print("==>", data)
        await Job.add(self.redis, prefix=self.prefix, **data)
        return web.Response(text="OK")

    async def reset_handler(self, request: web.Request):
        data = await request.json()
        # print("==>", data)
        await Job.reset(self.redis, **data, prefix=self.prefix)
        return web.Response(text="OK")

    async def remove_handle(self, request: web.Request):
        data = await request.json()
        # print("remove ==>", data)
        await Job.remove(self.redis, **data, prefix=self.prefix)
        return web.Response(text="OK")

    async def take_handler(self, request):
        data = await request.json()
        # print("take ==> data", data)
        job_id, payload = await Job.take(self.redis, **data, prefix=self.prefix)
        if payload:
            msg = msgpack.packb(
                {"job_id": job_id, "payload": payload}, use_bin_type=True
            )
            return web.Response(body=msg, status=200)
        else:
            return web.Response(text="EMPTY", status=200)

    def run(self, kill=None, *args, **kwargs):
        import os

        if kill or self.free_port:
            import time
            from killport import kill_ports

            kill_ports(ports=[self.port])
            time.sleep(0.01)

        # use the same endpoint for websocket and file serving.
        self._route("/queues", self.create_queue, method="PUT")
        self._route("/jobs", self.add_job, method="PUT")
        self._route("/jobs", self.take_handler, method="POST")
        self._route("/jobs/reset", self.reset_handler, method="POST")
        self._route("/jobs", self.remove_handle, method="DELETE")

        # serve local files via /static endpoint
        self._static("/static", self.static_root)
        print("Serving file://" + os.path.abspath(self.static_root), "at", "/static")

        print("redis server is now connected")
        print(f"serving at http://localhost:{self.port}")
        super().run()


if __name__ == "__main__":
    TaskServer().run()
