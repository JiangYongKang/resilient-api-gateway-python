"""Aggregation gateway entrypoint.

Run the gateway:        uv run python main.py
Run mock upstreams:     uv run python -m gateway.upstreams
"""
from __future__ import annotations

import os
from contextlib import asynccontextmanager

import httpx
import uvicorn
from fastapi import FastAPI

from gateway.aggregator import Aggregator
from gateway.breaker import CircuitBreaker
from gateway.config import GatewayConfig, UpstreamConfig
from gateway.models import AggregateResponse


def load_config() -> GatewayConfig:
    """Build config from environment variables with local defaults."""
    raw = os.getenv(
        "GATEWAY_UPSTREAMS",
        "alpha=http://127.0.0.1:9001/upstream/alpha/data,"
        "beta=http://127.0.0.1:9002/upstream/beta/data,"
        "gamma=http://127.0.0.1:9003/upstream/gamma/data",
    )
    upstreams = [
        UpstreamConfig(name=pair.split("=", 1)[0], url=pair.split("=", 1)[1])
        for pair in raw.split(",")
        if "=" in pair
    ]
    return GatewayConfig(
        upstreams=upstreams,
        request_timeout_s=float(os.getenv("GATEWAY_REQUEST_TIMEOUT_S", "2.0")),
        total_budget_s=float(os.getenv("GATEWAY_TOTAL_BUDGET_S", "3.0")),
        breaker_failure_threshold=int(os.getenv("GATEWAY_BREAKER_FAILURE_THRESHOLD", "3")),
        breaker_cooldown_s=float(os.getenv("GATEWAY_BREAKER_COOLDOWN_S", "5.0")),
        max_concurrent_per_upstream=int(os.getenv("GATEWAY_MAX_CONCURRENT_PER_UPSTREAM", "5")),
    )


@asynccontextmanager
async def lifespan(app: FastAPI):
    config = load_config()
    client = httpx.AsyncClient()
    breakers = {
        u.name: CircuitBreaker(u.name, config.breaker_failure_threshold,
                               config.breaker_cooldown_s)
        for u in config.upstreams
    }
    app.state.aggregator = Aggregator(config, breakers, client)
    app.state.breakers = breakers
    yield
    await client.aclose()


app = FastAPI(title="resilient-api-gateway", lifespan=lifespan)


@app.get("/aggregate", response_model=AggregateResponse)
async def aggregate() -> AggregateResponse:
    return await app.state.aggregator.aggregate()


@app.get("/breakers")
async def breakers() -> dict:
    return {
        name: {
            "state": b.state.value,
            "consecutive_failures": b.consecutive_failures,
        }
        for name, b in app.state.breakers.items()
    }


@app.get("/health")
async def health() -> dict:
    return {"status": "ok"}


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
