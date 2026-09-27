from __future__ import annotations

import logging

import httpx
import pytest

from gateway.aggregator import Aggregator
from gateway.breaker import CircuitBreaker
from gateway.config import GatewayConfig, UpstreamConfig
from gateway.upstreams import ModeRequest, create_mock_upstream_app

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

NAMES = ["alpha", "beta", "gamma"]


@pytest.fixture()
def mock_app():
    return create_mock_upstream_app(NAMES)


@pytest.fixture()
def config() -> GatewayConfig:
    return GatewayConfig(
        upstreams=[
            UpstreamConfig(name=n, url=f"http://mock/upstream/{n}/data")
            for n in NAMES
        ],
        request_timeout_s=0.3,
        total_budget_s=0.8,
        breaker_failure_threshold=3,
        breaker_cooldown_s=0.2,
        max_concurrent_per_upstream=3,
    )


@pytest.fixture()
def breakers(config) -> dict[str, CircuitBreaker]:
    return {
        u.name: CircuitBreaker(u.name, config.breaker_failure_threshold,
                               config.breaker_cooldown_s)
        for u in config.upstreams
    }


@pytest.fixture()
async def aggregator(config, breakers, mock_app):
    client = httpx.AsyncClient(
        transport=httpx.ASGITransport(app=mock_app), base_url="http://mock"
    )
    yield Aggregator(config, breakers, client)
    await client.aclose()


def set_mode(mock_app, name: str, mode: str, delay_s: float = 0.0) -> None:
    mock_app.state.modes[name] = ModeRequest(mode=mode, delay_s=delay_s)


def report(resp, breakers) -> None:
    """Print every upstream's result and breaker state for the test log."""
    for name, r in resp.results.items():
        print(f"  upstream={name} status={r.status.value} "
              f"reason={r.reason} latency_ms={r.latency_ms}")
    for name, b in breakers.items():
        print(f"  breaker={name} state={b.state.value} "
              f"consecutive_failures={b.consecutive_failures}")
