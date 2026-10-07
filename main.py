"""本地一键演示启动：3 个 mock 上游 + 聚合网关，全部绑定 127.0.0.1。

    uv run python main.py

默认：
- users    -> 正常
- orders   -> 正常（稍慢）
- payments -> 持续 500（用于演示部分失败与熔断）

可用 GW_TOTAL_TIMEOUT / GW_UPSTREAM_TIMEOUT / GW_BREAKER_THRESHOLD /
GW_BREAKER_RECOVERY / GW_MAX_CONCURRENCY / GW_PROBE_* 等环境变量调整。
启动后访问 http://127.0.0.1:8000/aggregate 、 /circuits 与 /recovery
（/recovery 可观察 payments 熔断后被后台探测自动恢复的进展）。
"""

from __future__ import annotations

import logging
import signal
import sys

import uvicorn

from gateway.app import create_app
from gateway.config import build_config, UpstreamConfig
from gateway.mock_server import MockServer
from gateway.mock_upstream import create_mock_app

GATEWAY_HOST = "127.0.0.1"
GATEWAY_PORT = 8000


def _start_local_mocks() -> list[MockServer]:
    specs = [
        ("users", create_mock_app("users", mode="ok", delay=0.0)),
        ("orders", create_mock_app("orders", mode="slow", delay=0.2)),
        ("payments", create_mock_app("payments", mode="error")),
    ]
    servers: list[MockServer] = []
    for name, app in specs:
        server = MockServer(app)
        server.start()
        servers.append(server)
        logging.info("mock upstream %-10s listening on %s", name, server.base_url)
    return servers


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        stream=sys.stdout,
    )

    servers = _start_local_mocks()
    try:
        upstreams = tuple(
            UpstreamConfig(name=name, base_url=server.base_url, timeout=1.0)
            for name, server in zip(("users", "orders", "payments"), servers)
        )
        config = build_config(upstreams)
        app = create_app(config)

        config_server = uvicorn.Config(
            app, host=GATEWAY_HOST, port=GATEWAY_PORT, log_level="info")
        server = uvicorn.Server(config_server)

        def _stop(*_args) -> None:
            server.should_exit = True

        signal.signal(signal.SIGTERM, _stop)
        signal.signal(signal.SIGINT, _stop)
        server.run()
    finally:
        for mock in servers:
            mock.stop()


if __name__ == "__main__":
    main()
