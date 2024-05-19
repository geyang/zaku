import asyncio
import ssl
import traceback
from collections.abc import Coroutine
from concurrent.futures import CancelledError
from functools import partial
from pathlib import Path

import aiohttp_cors
from aiohttp import web


async def default_handler(request, ws):
    async for msg in ws:
        print(msg)


async def websocket_handler(request, handler, **ws_kwargs):
    ws = web.WebSocketResponse(**ws_kwargs)
    await ws.prepare(request)

    try:
        await handler(request, ws)

    except ConnectionResetError:
        print("Connection reset")

    except CancelledError:
        print("WebSocket Canceled")

    except Exception as exp:
        print(f"Error:\n{exp}\n{traceback.print_exc()}")

    finally:
        await ws.close()
        print("WebSocket connection closed")


async def handle_file_request(request, root, filename=None):
    if filename is None:
        filename = request.match_info["filename"]

    filepath = Path(root) / filename

    if not filepath.is_file():
        raise web.HTTPNotFound()

    return web.FileResponse(filepath)


class Server:
    """Base TCP server"""

    host: str = "localhost"
    port: int = 8012
    cors: str = "*"
    "Enable CORS"

    cert: str = None
    "the path to the SSL certificate"
    key: str = None
    "the path to the SSL key"
    ca_cert: str = None
    "the trusted root CA certificates"

    WEBSOCKET_MAX_SIZE = 2**28
    REQUEST_MAX_SIZE = 2**28

    def __post_init__(self):
        self.app = web.Application(client_max_size=self.REQUEST_MAX_SIZE)

        default = aiohttp_cors.ResourceOptions(
            allow_credentials=True,
            expose_headers="*",
            allow_headers="*",
            allow_methods="*",
        )
        cors_config = {k: default for k in self.cors.split(",")}

        self.cors_context = aiohttp_cors.setup(self.app, defaults=cors_config)

    def _route(
        self,
        path: str,
        handler: callable,
        method: str = "GET",
    ):
        route = self.app.router.add_resource(path).add_route(method, handler)
        self.cors_context.add(route)

    def _socket(self, path: str, handler: callable):
        ws_handler = partial(
            websocket_handler, handler=handler, max_msg_size=self.WEBSOCKET_MAX_SIZE
        )
        self._route(path, ws_handler)

    @staticmethod
    def _add_task(fn: Coroutine, name=None):
        loop = asyncio.get_event_loop()
        loop.create_task(fn, name=name)

    def _static(self, path, root):
        _fn = partial(handle_file_request, root=root)
        self._route(f"{path}/{{filename:.*}}", _fn, method="GET")

    def _static_file(self, path, root, filename=None):
        _fn = partial(handle_file_request, root=root, filename=filename)
        self._route(f"{path}", _fn, method="GET")

    def run(self):
        """Simple Runner

        When using gunicorn, this gets replace by the gunicorn runner. gunicorn
        is responsible for handling the ssl certification, and the port binding.

        ```shell
        $ gunicorn --certfile=server.crt --keyfile=server.key --bind 0.0.0.0:443 test:app
        ```
        """
        async def init_server():
            runner = web.AppRunner(self.app)
            await runner.setup()
            if not self.cert:
                site = web.TCPSite(runner, self.host, self.port)
                return await site.start()

            ssl_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
            ssl_context.load_cert_chain(certfile=self.cert, keyfile=self.key)
            if self.ca_cert:
                ssl_context.load_verify_locations(self.ca_cert)
                ssl_context.verify_mode = ssl.CERT_REQUIRED
            else:
                ssl_context.verify_mode = ssl.CERT_OPTIONAL

            site = web.TCPSite(runner, self.host, self.port, ssl_context=ssl_context)
            return await site.start()

        event_loop = asyncio.get_event_loop()

        event_loop.run_until_complete(init_server())
        event_loop.run_forever()


if __name__ == "__main__":
    app = Server()
    app.run()
