"""Data models for the aggregation gateway."""
from __future__ import annotations

import enum
from typing import Any

from pydantic import BaseModel


class UpstreamStatus(str, enum.Enum):
    """Outcome status of a single upstream call."""

    OK = "ok"
    TIMEOUT = "timeout"
    ERROR = "error"
    MALFORMED = "malformed"
    CIRCUIT_OPEN = "circuit_open"


class UpstreamResult(BaseModel):
    """Result of calling one upstream."""

    name: str
    status: UpstreamStatus
    data: Any = None
    reason: str | None = None
    latency_ms: float | None = None


class AggregateResponse(BaseModel):
    """Merged response returned to the client."""

    results: dict[str, UpstreamResult]
    succeeded: list[str]
    failed: list[str]
