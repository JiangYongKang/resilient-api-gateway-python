"""FastAPI 网关应用工厂。

端点：
- GET /aggregate      并发聚合全部上游，返回部分结果 + 失败标记
- GET /health         网关存活
- GET /circuits       各上游熔断器当前状态（可观测）
"""

from __future__ import annotations

import logging

from fastapi import FastAPI

from gateway.aggregator import Aggregator
from gateway.breaker import CircuitBreakerRegistry
from gateway.config import GatewayConfig

logger = logging.getLogger("gateway")


def create_app(config: GatewayConfig) -> FastAPI:
    registry = CircuitBreakerRegistry(config.breaker)
    aggregator = Aggregator(config, registry)

    app = FastAPI(title="resilient-aggregate-gateway")
    app.state.config = config
    app.state.registry = registry
    app.state.aggregator = aggregator

    @app.get("/")
    def root() -> dict:
        return {
            "service": "resilient-aggregate-gateway",
            "upstreams": [u.name for u in config.upstreams],
            "total_timeout": config.total_timeout,
        }

    @app.get("/health")
    def health() -> dict:
        return {"status": "ok"}

    @app.get("/aggregate")
    async def aggregate() -> dict:
        """聚合所有上游。

        只要有至少一个上游成功即返回 200（partial success 在响应体中标记）；
        全部失败时返回 503，但响应体结构不变，失败原因完整保留。
        """
        response = await aggregator.aggregate()
        payload = response.model_dump()
        return payload if not response.all_failed else \
            _json_response_503(payload)

    @app.get("/circuits")
    def circuits() -> dict:
        return {
            "states": {name: state.value
                       for name, state in registry.states().items()},
            "snapshots": registry.snapshots(),
            "config": {
                "failure_threshold": config.breaker.failure_threshold,
                "recovery_timeout": config.breaker.recovery_timeout,
            },
        }

    return app


def _json_response_503(payload: dict):
    from fastapi.responses import JSONResponse

    return JSONResponse(status_code=503, content=payload)
