"""在后台线程中启动本地 mock 上游（真实 127.0.0.1 HTTP，无外部网络）。

每个 server 跑在独立线程 + 独立 asyncio 事件循环里，
多个上游因此可以有不同端口、互不干扰。
"""

from __future__ import annotations

import contextlib
import socket
import threading
import time

import httpx
import uvicorn


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def _wait_ready(url: str, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    last_exc: Exception | None = None
    while time.monotonic() < deadline:
        try:
            with httpx.Client(timeout=0.5) as client:
                resp = client.get(url)
                resp.raise_for_status()
                return
        except Exception as exc:  # 服务尚未就绪
            last_exc = exc
            time.sleep(0.02)
    raise RuntimeError(f"mock server at {url} not ready: {last_exc}")


class MockServer:
    def __init__(self, app, host: str = "127.0.0.1", port: int | None = None,
                 startup_timeout: float = 5.0) -> None:
        self.app = app
        self.host = host
        self.port = port or free_port()
        self.startup_timeout = startup_timeout
        self._server: uvicorn.Server | None = None
        self._thread: threading.Thread | None = None

    @property
    def base_url(self) -> str:
        return f"http://{self.host}:{self.port}"

    def start(self) -> str:
        config = uvicorn.Config(
            self.app, host=self.host, port=self.port,
            log_level="error", lifespan="on",
        )
        server = uvicorn.Server(config)
        server.install_signal_handlers = lambda: None  # 子线程不接管信号
        self._server = server

        def _run() -> None:
            asyncio_run(server.serve())

        self._thread = threading.Thread(
            target=_run, name=f"mock-{self.port}", daemon=True)
        self._thread.start()
        _wait_ready(f"{self.base_url}/health", self.startup_timeout)
        return self.base_url

    def stop(self) -> None:
        if self._server is not None:
            self._server.should_exit = True
        if self._thread is not None:
            self._thread.join(timeout=5)
        self._server = None
        self._thread = None

    def __enter__(self) -> "MockServer":
        self.start()
        return self

    def __exit__(self, *exc) -> None:
        with contextlib.suppress(Exception):
            self.stop()


def asyncio_run(coro):
    """在线程入口创建并运行独立事件循环。"""
    import asyncio

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(coro)
    finally:
        loop.close()
