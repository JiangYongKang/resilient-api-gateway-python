"""测试公共夹具：真实 127.0.0.1 mock 上游 + 网关 in-process ASGI 客户端。

- 上游是后台线程中的真实 uvicorn HTTP 服务（多个端口），覆盖真实超时/连接行为；
- 网关本身走 ASGITransport 直连，不需要占用端口；
- 不依赖任何外部网络。
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass

import httpx
import pytest
import pytest_asyncio

from gateway.app import create_app
from gateway.config import build_config, UpstreamConfig
from gateway.mock_server import MockServer
from gateway.mock_upstream import create_mock_app

# 让“每个上游结果/熔断状态变化”日志在测试输出中可见
logging.getLogger("gateway").setLevel(logging.INFO)
if not any(isinstance(h, logging.StreamHandler) for h in logging.getLogger().handlers):
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s %(message)s")


DEFAULT_UPSTREAMS = ("users", "orders", "payments")


@dataclass
class MockCluster:
    servers: dict[str, MockServer]

    def set_mode(self, name: str, mode: str, delay: float | None = None,
                 sequence: list[str] | None = None) -> None:
        body: dict = {"mode": mode}
        if delay is not None:
            body["delay"] = delay
        if sequence is not None:
            body["sequence"] = sequence
        resp = httpx.post(f"{self.servers[name].base_url}/control",
                          json=body, timeout=2)
        assert resp.status_code == 200, resp.text

    def mode_info(self, name: str) -> dict:
        return httpx.get(f"{self.servers[name].base_url}/control",
                         timeout=2).json()

    def stop(self) -> None:
        for server in self.servers.values():
            server.stop()


@pytest.fixture
def mock_cluster() -> MockCluster:
    servers: dict[str, MockServer] = {}
    for name in DEFAULT_UPSTREAMS:
        server = MockServer(create_mock_app(name, mode="ok"))
        server.start()
        servers[name] = server
    cluster = MockCluster(servers)
    try:
        yield cluster
    finally:
        cluster.stop()


@dataclass
class GatewayHandle:
    app: object
    client: httpx.AsyncClient
    cluster: MockCluster

    async def aggregate(self) -> httpx.Response:
        return await self.client.get("/aggregate")

    async def circuits(self) -> dict:
        return (await self.client.get("/circuits")).json()


@pytest_asyncio.fixture
async def gateway_factory(mock_cluster):
    handles: list[GatewayHandle] = []

    async def _make(total_timeout: float = 2.0,
                    upstream_timeout: float = 1.0,
                    failure_threshold: int = 3,
                    recovery_timeout: float = 1.0,
                    max_concurrency: int = 8,
                    upstream_overrides: dict[str, dict] | None = None) -> GatewayHandle:
        upstream_overrides = upstream_overrides or {}
        upstreams = tuple(
            UpstreamConfig(
                name=name,
                base_url=mock_cluster.servers[name].base_url,
                timeout=upstream_overrides.get(name, {}).get("timeout",
                                                             upstream_timeout),
            )
            for name in DEFAULT_UPSTREAMS
        )
        config = build_config(
            upstreams,
            total_timeout=total_timeout,
            default_upstream_timeout=upstream_timeout,
            max_concurrent_calls=max_concurrency,
            failure_threshold=failure_threshold,
            recovery_timeout=recovery_timeout,
        )
        app = create_app(config)
        transport = httpx.ASGITransport(app=app)
        client = httpx.AsyncClient(transport=transport, base_url="http://gw")
        handle = GatewayHandle(app=app, client=client, cluster=mock_cluster)
        handles.append(handle)
        return handle

    yield _make

    for handle in handles:
        await handle.client.aclose()


def print_report(label: str, body: dict, circuits: dict | None = None) -> None:
    """测试日志：打印每个上游结果与当前熔断状态。"""
    print(f"\n===== {label} =====")
    for r in body.get("results", []):
        status = "OK  " if r["ok"] else f"FAIL[{(r['reason'] or '?'):>16}] degraded={r['degraded']}"
        print(f"  upstream={r['upstream']:<10} {status} "
              f"elapsed_ms={r['elapsed_ms']:<5} circuit={r['circuit_state']} "
              f"data={r['data']} error={r['error']}")
    print(f"  success={body.get('success')} all_failed={body.get('all_failed')} "
          f"timed_out={body.get('timed_out')} total_ms={body.get('total_ms')}")
    if circuits:
        print(f"  circuits={circuits.get('states')}")
