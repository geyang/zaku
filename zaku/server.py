import msgpack
import redis
from aiohttp import web
from params_proto import Proto, ParamsProto, Flag

from zaku.base import Server
from zaku.interfaces import Job


class Redis(ParamsProto, prefix="redis", cli_parse=False):
    """Redis Configuration for the TaskServer class.

    .. code-block:: shell

        # Put this into an .env file
        REDIS_HOST=localhost
        REDIS_PORT=6379
        REDIS_PASSWORD=xxxxxxxxxxxxxxxxxxx
        REDIS_DB=0

    CLI Options::

        --redis.host      :str 'localhost'
        --redis.port      :int 6379
        --redis.password  :any None
        --redis.db        :any 0
    """

    host: str = Proto("localhost", env="REDIS_HOST")
    port: int = Proto(6379, env="REDIS_PORT")
    password: str = Proto(env="REDIS_PASSWORD")
    db: int = Proto(0, env="REDIS_DB")
    """The logical redis database, from 0 - 15. """


class TaskServer(ParamsProto, Server):
    """TaskServer

    This is the server that maintains the Task Queue.

    Usage

    .. code-block:: python

        app = TaskServer()
        app.run()

    CLI Options

    .. code-block:: shell

        python -m zaku.server --help

        -h, --help            show this help message and exit
        --prefix            :str 'Zaku-task-queues'
        --queue-len         :int 100
        --host              :str 'localhost'
                              set to 0.0.0.0 to enable remote (not localhost) connections.
        --port              :int 9000
        --cors              :str 'https://vuer.ai,https://dash.ml,http://lo...
        --cert              :str None the path to the SSL certificate
        --key               :str None the path to the SSL key
        --ca-cert           :str None the trusted root CA certificates
        --REQUEST-MAX-SIZE  :int 100000000 the maximum packet size
        --free-port         :bool False
                              kill process squatting target port if True.
        --static-root       :str '.'
        --verbose           :bool False
                              show the list of configurations during launch if True.

    """

    prefix = "Zaku-task-queues"

    # Queue Parameters
    queue_len = 100  # use a max length to avoid the memory from blowing up.

    # Server Parameters
    host: str = Proto(
        "localhost",
        help="set to 0.0.0.0 to enable remote (not localhost) connections.",
    )
    port: int = 9000
    cors: str = (
        "https://vuer.ai,https://dash.ml,http://localhost:8000,http://127.0.0.1:8000,*"
    )

    # SSL Parameters
    cert: str = Proto(None, dtype=str, help="the path to the SSL certificate")
    key: str = Proto(None, dtype=str, help="the path to the SSL key")
    ca_cert: str = Proto(None, dtype=str, help="the trusted root CA certificates")

    REQUEST_MAX_SIZE: int = Proto(
        100_000_000, env="WEBSOCKET_MAX_SIZE", help="the maximum packet size"
    )

    free_port: bool = Flag("kill process squatting target port if True.")
    static_root: str = "."
    verbose = Flag("show the list of configurations during launch if True.")

    def __post_init__(self):
        Server.__post_init__(self)

        if self.verbose:
            for k, v in vars(self).items():
                print(f"{k}: {v},")

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
        self._route("/tasks", self.add_job, method="PUT")
        self._route("/tasks", self.take_handler, method="POST")
        self._route("/tasks/reset", self.reset_handler, method="POST")
        self._route("/tasks", self.remove_handle, method="DELETE")

        # serve local files via /static endpoint
        self._static("/static", self.static_root)
        print("Serving file://" + os.path.abspath(self.static_root), "at", "/static")

        print("redis server is now connected")
        print(f"serving at http://localhost:{self.port}")
        super().run()


def entry_point():
    TaskServer().run()


if __name__ == "__main__":
    entry_point()
