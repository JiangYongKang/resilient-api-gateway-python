"""Configuration for the aggregation gateway."""
from __future__ import annotations

from pydantic import BaseModel


class UpstreamConfig(BaseModel):
    """One upstream service endpoint."""

    name: str
    url: str


class GatewayConfig(BaseModel):
    """Tunable knobs for aggregation, breaking and concurrency."""

    upstreams: list[UpstreamConfig] = []
    request_timeout_s: float = 2.0
    total_budget_s: float = 3.0
    breaker_failure_threshold: int = 3
    breaker_cooldown_s: float = 5.0
    max_concurrent_per_upstream: int = 5
