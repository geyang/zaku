import msgpack
import redis
from aiohttp import web
from params_proto import Proto, ParamsProto, Flag
from dotenv import load_dotenv
from base import Server
from interfaces import Job

load_dotenv()


class Redis(ParamsProto, prefix="redis", cli_parse=False):
    """Redis Configuration for the TaskServer class.

    We support both direct connection to a single redis server,
        and a high availability setup using Redis Sentinel.
        The latter require setting up a replica set
        and a group of sentinels.

    Single Redis Server Usage
    =========================

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

    Sentinel Usage (High availability setup)
    ========================================

    .. code-block:: shell

        # Put this into an .env file
        SENTINEL_HOSTs=host1:port1,host2:port2  # <=== Notice it is plural.
                                                #   This is a comma separated list of hosts.
        SENTINEL_PASSWORD=xxxxxxxxxxxxxxxxxxx.  # <=== This is the password for the sentinels.

        sentinel_cluster_name=primary           # <=== This is the name of the redis cluster.
        SENTINEL_DB=0                           # <=== This is the logical database.

    CLI Options::

        --redis.sentinel_hosts     :str 'localhost'
        --redis.sentinel_password  :any None
        --redis.sentinel_db        :any 0

    Common Errors:
    ==============

    Setting keepalive: ConnectionError is due to timeout. See below for more infomation.

    https://devcenter.heroku.com/articles/ah-redis-stackhero#:~:text=The%20error%20%E2%80%9Credis.,and%20the%20connection%20closes%20automatically
    """

    host: str = Proto("localhost", env="REDIS_HOST")
    port: int = Proto(6379, env="REDIS_PORT")
    password: str = Proto(env="REDIS_PASSWORD")

    # todo: add support for cluster mode without sentinel.
    sentinel_hosts: str = Proto(
        env="SENTINEL_HOSTS",
        help="comma separated list of redis hosts. Example: host1:port1,host2:port2,host3:port3."
    )
    sentinel_password: str = Proto(password, env="SENTINEL_PASSWORD")

    cluster_name = Proto("primary", env="SENTINEL_CLUSTER_NAME")
    shuffle: bool = Proto(False, env="REDIS_SHUFFLE",
                          help="shuffle the sentinel hosts on init.")

    db: int = Proto(0, env="REDIS_DB",
                    help="The logical redis database, from 0 - 15.")

    # connection arguments
    health_check_interval = 10
    socket_connect_timeout = 5
    retry_on_timeout = True
    socket_keepalive = True

    def __post_init__(self, _deps=None):
        if self.sentinel_hosts:
            import random

            self.sentinel_hosts = [h.split(":") for h in
                                   self.sentinel_hosts.split(",")]

            if self.shuffle:
                self.sentinel_hosts = random.shuffle(self.sentinel_hosts,
                                                     key=lambda x: hash(x))

            self.sentinel = redis.asyncio.sentinel.Sentinel(
                self.sentinel_hosts,
                # redis_class=RobustRedis,
                password=self.sentinel_password,
                health_check_interval=self.health_check_interval,
                socket_connect_timeout=self.socket_connect_timeout,
                retry_on_timeout=self.retry_on_timeout,
                socket_keepalive=self.socket_keepalive,
            )

            self.connection = self.sentinel.master_for(self.cluster_name,
                                                       password=self.password,
                                                       db=self.db)

        else:
            # self.connection = RobustRedis(password=self.password, db=self.db, host=self.host, port=self.port)
            self.connection = redis.asyncio.Redis(
                password=self.password,
                db=self.db,
                host=self.host,
                port=self.port,
                health_check_interval=self.health_check_interval,
                socket_connect_timeout=self.socket_connect_timeout,
                retry_on_timeout=self.retry_on_timeout,
                socket_keepalive=self.socket_keepalive,
                ssl=True
            )


class MongoDB(ParamsProto, prefix="mongo", cli_parse=False):
    """MongoDB Configuration for the TaskServer class.

    We support both direct connection to a single MongoDB server,
        and a high availability setup using MongoDB replica sets.

    Single MongoDB Server Usage
    ==========================

    .. code-block:: shell

        # Put this into an .env file
        MONGO_HOST=localhost
        MONGO_PORT=27017
        MONGO_USERNAME=admin
        MONGO_PASSWORD=xxxxxxxxxxxxxxxxxxx
        MONGO_DATABASE=zaku
        MONGO_AUTH_SOURCE=admin

    CLI Options::

        --mongo.host           :str 'localhost'
        --mongo.port           :int 27017
        --mongo.username       :str None
        --mongo.password       :str None
        --mongo.database       :str 'zaku'
        --mongo.auth_source    :str 'admin'

    Replica Set Usage (High availability setup)
    ===========================================

    .. code-block:: shell

        # Put this into an .env file
        MONGO_URI=mongodb://host1:port1,host2:port2,host3:port3/?replicaSet=rs0
        MONGO_DATABASE=zaku
        MONGO_USERNAME=admin
        MONGO_PASSWORD=xxxxxxxxxxxxxxxxxxx
        MONGO_AUTH_SOURCE=admin

    CLI Options::

        --mongo.uri            :str None
        --mongo.database       :str 'zaku'
        --mongo.username       :str None
        --mongo.password       :str None
        --mongo.auth_source    :str 'admin'
    """

    host: str = Proto("localhost", env="MONGO_HOST")
    port: int = Proto(27017, env="MONGO_PORT")
    username: str = Proto(env="root")
    password: str = Proto(env="root")
    database: str = Proto("zaku", env="MONGO_DATABASE")
    auth_source: str = Proto("admin", env="MONGO_AUTH_SOURCE")

    # For replica set connections
    uri: str = Proto(env="MONGO_URI",
                     help="MongoDB connection URI for replica sets")

    def __post_init__(self, _deps=None):
        if self.uri:
            # Use connection URI if provided (for replica sets)
            if self.username and self.password:
                # Replace username and password in URI if provided
                uri_parts = self.uri.split("://")
                if len(uri_parts) == 2:
                    protocol, rest = uri_parts
                    if "@" in rest:
                        # URI already has authentication
                        self.connection_string = self.uri
                    else:
                        # Add authentication to URI
                        auth_part = f"{self.username}:{self.password}@"
                        host_part = rest.split("/")[0]
                        db_part = "/".join(rest.split("/")[1:])
                        self.connection_string = f"{protocol}://{auth_part}{host_part}/{db_part}"
                else:
                    self.connection_string = self.uri
            else:
                self.connection_string = self.uri
        else:
            # Build connection string from individual components
            if self.username and self.password:
                self.connection_string = f"mongodb://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}?authSource={self.auth_source}"
            else:
                self.connection_string = f"mongodb://{self.host}:{self.port}/{self.database}"


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
        "0.0.0.0",
        help="set to 0.0.0.0 to enable remote (not localhost) connections.",
        env="ZAKU_HOST"
    )
    port: int = 9001
    cors: str = "https://vuer.ai,https://dash.ml,http://localhost:8000,http://127.0.0.1:8000,*"

    # SSL Parameters
    cert: str = Proto(None, dtype=str, help="the path to the SSL certificate")
    key: str = Proto(None, dtype=str, help="the path to the SSL key")
    ca_cert: str = Proto(None, dtype=str,
                         help="the trusted root CA certificates")

    REQUEST_MAX_SIZE: int = Proto(100_000_000, env="WEBSOCKET_MAX_SIZE",
                                  help="the maximum packet size")

    free_port: bool = Flag("kill process squatting target port if True.")
    static_root: str = "."
    verbose = Flag("show the list of configurations during launch if True.")

    def print_info(self):
        print("========= Arguments =========")
        for k, v in vars(self).items():
            print(f" {k} = {v},")
        print("-----------------------------")

    def __post_init__(self, _deps=None):
        if self.verbose:
            self.print_info()

        Server.__post_init__(self)

        self.redis_wrapper = Redis(_deps)
        self.redis = self.redis_wrapper.connection

        # Initialize MongoDB for payload storage
        self.mongo_wrapper = MongoDB(_deps)
        self.mongo_initialized = False

    async def create_queue(self, request: web.Request):
        data = await request.json()
        try:
            await Job.create_queue(self.redis, **data, prefix=self.prefix)
        except Exception as e:
            # the error handling here is not ideal.
            return web.Response(text="ERROR: " + str(e), status=200)

        return web.Response(text="OK")

    async def add_job(self, request: web.Request):
        msg = await request.read()
        data = msgpack.unpackb(msg)
        # print("==>", data)
        await Job.add(self.redis, prefix=self.prefix, **data)
        return web.Response(text="OK")

    async def publish_job(self, request: web.Request):
        msg = await request.read()
        data = msgpack.unpackb(msg)
        # print("==>", data)
        num_subscribers = await Job.publish(self.redis, prefix=self.prefix,
                                            **data)
        # todo: return the number of subscribers.
        return web.Response(text=str(num_subscribers), status=200)

    async def reset_handler(self, request: web.Request):
        data = await request.json()
        # print("==>", data)
        await Job.reset(self.redis, **data, prefix=self.prefix)
        return web.Response(text="OK")

    async def remove_handler(self, request: web.Request):
        data = await request.json()
        # print("remove ==>", data)
        await Job.remove(self.redis, **data, prefix=self.prefix)
        return web.Response(text="OK")

    async def count_files_handler(self, request):
        data = await request.json()

        try:
            counts = await Job.count_files(self.redis, **data,
                                           prefix=self.prefix)
        except redis.exceptions.ResponseError as e:
            if "no such index" in str(e):
                return web.Response(status=200)
                # return web.Response(text="no such index", status=404)
            raise e

        msg = msgpack.packb({"counts": counts}, use_bin_type=True)
        return web.Response(body=msg, status=200)

    async def take_handler(self, request):
        data = await request.json()

        try:
            job_id, payload = await Job.take(self.redis, **data,
                                             prefix=self.prefix)
        except redis.exceptions.ResponseError as e:
            if "no such index" in str(e):
                return web.Response(status=200)
                # return web.Response(text="no such index", status=404)
            raise e

        if payload:
            msg = msgpack.packb({"job_id": job_id, "payload": payload},
                                use_bin_type=True)
            return web.Response(body=msg, status=200)

        return web.Response(status=200)

    async def unstale_handler(self, request: web.Request):
        data = await request.json()
        # print("take ==> data", data)
        await Job.unstale_tasks(self.redis, **data, prefix=self.prefix)

        return web.Response(text="OK", status=200)

    async def subscribe_one_handler(self, request):
        data = await request.json()

        payload = await Job.subscribe(self.redis, **data, prefix=self.prefix)

        if payload:
            return web.Response(body=payload, status=200)

        return web.Response(status=200)

    async def subscribe_streaming_handler(self, request) -> web.StreamResponse:
        data = await request.json()

        async def stream_response(response):
            try:
                async for payload in Job.subscribe_stream(self.redis, **data,
                                                          prefix=self.prefix):
                    # use msgpack.Unpacker to determin the end of message.
                    await response.write(payload)

                await response.write_eof()

            except ConnectionResetError:
                print("client disconnected.")
                return response

            return response

        response = web.StreamResponse(
            status=200,
            reason="OK",
            headers={"Content-Type": "text/plain"},
        )
        await response.prepare(request)
        await stream_response(response)
        return response

    def setup_server(self):
        import os

        # use the same endpoint for websocket and file serving.
        self._route("/queues", self.create_queue, method="PUT")
        self._route("/tasks", self.add_job, method="PUT")
        self._route("/tasks", self.take_handler, method="POST")
        self._route("/tasks/counts", self.count_files_handler, method="GET")
        self._route("/tasks/reset", self.reset_handler, method="POST")
        self._route("/tasks", self.remove_handler, method="DELETE")
        self._route("/tasks/unstale", self.unstale_handler, method="PUT")

        self._route("/publish", self.publish_job, method="PUT")
        self._route("/subscribe_one", self.subscribe_one_handler,
                    method="POST")
        self._route("/subscribe_stream", self.subscribe_streaming_handler,
                    method="POST")

        # serve local files via /static endpoint
        self._static("/static", self.static_root)
        print("Serving file://" + os.path.abspath(self.static_root), "at",
              "/static")

        return self.app

    async def initialize_mongodb(self):
        """Initialize MongoDB connection"""
        try:
            from mongo_helpers import MongoManager
            await MongoManager.initialize(
                self.mongo_wrapper.connection_string,
                self.mongo_wrapper.database
            )
            self.mongo_initialized = True
            print("MongoDB server is now connected")
        except Exception as e:
            print(f"Warning: Failed to initialize MongoDB: {e}")
            print("Payload storage will not be available")
            self.mongo_initialized = False

    def run(self, kill=None, *args, **kwargs):
        if kill or self.free_port:
            import time
            from killport import kill_ports

            kill_ports(ports=[self.port])
            time.sleep(0.01)

        self.setup_server()

        # Initialize MongoDB asynchronously
        import asyncio
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

        loop.run_until_complete(self.initialize_mongodb())

        print("redis server is now connected")
        print(f"serving at http://localhost:{self.port}")
        super().run()


def entry_point():
    TaskServer().run()


async def get_app():
    """This is sued by gunicorn to run the server in worker mode."""
    return TaskServer().setup_server()


if __name__ == "__main__":
    entry_point()
