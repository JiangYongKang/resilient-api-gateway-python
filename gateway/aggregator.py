"""Fan-out aggregation across upstreams."""
from __future__ import annotations

import asyncio
import logging
import time

import httpx

from .breaker import CircuitBreaker
from .config import GatewayConfig
from .models import AggregateResponse, UpstreamResult, UpstreamStatus

logger = logging.getLogger("gateway.aggregator")


class Aggregator:
    """Calls all upstreams concurrently and merges partial results.

    Guarantees:
    - Partial failure: one upstream failing never fails the whole request;
      failures are reported with an explicit status and reason.
    - Total time budget: the aggregate call returns within
      ``config.total_budget_s`` even if some upstreams are still pending;
      pending upstreams are cancelled and reported as timeouts.
    - Concurrency control: each upstream has its own semaphore limiting
      simultaneous in-flight requests to it.
    """

    def __init__(
        self,
        config: GatewayConfig,
        breakers: dict[str, CircuitBreaker],
        client: httpx.AsyncClient,
    ) -> None:
        self.config = config
        self.breakers = breakers
        self.client = client
        self._semaphores = {
            u.name: asyncio.Semaphore(config.max_concurrent_per_upstream)
            for u in config.upstreams
        }

    async def aggregate(self) -> AggregateResponse:
        tasks = {
            asyncio.ensure_future(self._call_upstream(u.name, u.url)): u.name
            for u in self.config.upstreams
        }
        done, pending = await asyncio.wait(
            tasks.keys(), timeout=self.config.total_budget_s
        )
        results: dict[str, UpstreamResult] = {}
        for task in done:
            results[tasks[task]] = task.result()
        for task in pending:
            task.cancel()
            name = tasks[task]
            breaker = self.breakers[name]
            breaker.record_failure()
            results[name] = UpstreamResult(
                name=name,
                status=UpstreamStatus.TIMEOUT,
                reason=(
                    f"exceeded total budget of {self.config.total_budget_s}s; "
                    "request cancelled"
                ),
            )
        results = dict(sorted(results.items()))
        return AggregateResponse(
            results=results,
            succeeded=[n for n, r in results.items() if r.status is UpstreamStatus.OK],
            failed=[n for n, r in results.items() if r.status is not UpstreamStatus.OK],
        )

    async def _call_upstream(self, name: str, url: str) -> UpstreamResult:
        breaker = self.breakers[name]
        if not breaker.allow_request():
            return UpstreamResult(
                name=name,
                status=UpstreamStatus.CIRCUIT_OPEN,
                reason=(
                    f"circuit breaker {breaker.state.value}: fast fail after "
                    f"{breaker.consecutive_failures} consecutive failures"
                ),
            )
        async with self._semaphores[name]:
            start = time.monotonic()
            try:
                resp = await self.client.get(
                    url, timeout=self.config.request_timeout_s
                )
                resp.raise_for_status()
                data = resp.json()
                if not isinstance(data, dict) or "payload" not in data:
                    raise ValueError("response missing required 'payload' field")
            except httpx.TimeoutException:
                return self._fail(breaker, name, UpstreamStatus.TIMEOUT,
                                  f"no response within {self.config.request_timeout_s}s",
                                  start)
            except httpx.HTTPStatusError as exc:
                return self._fail(breaker, name, UpstreamStatus.ERROR,
                                  f"upstream returned HTTP {exc.response.status_code}",
                                  start)
            except httpx.HTTPError as exc:
                return self._fail(breaker, name, UpstreamStatus.ERROR,
                                  f"connection error: {exc.__class__.__name__}", start)
            except (ValueError, KeyError) as exc:
                return self._fail(breaker, name, UpstreamStatus.MALFORMED,
                                  f"invalid response body: {exc}", start)
            latency = (time.monotonic() - start) * 1000
            breaker.record_success()
            logger.info("upstream[%s] ok in %.1fms", name, latency)
            return UpstreamResult(
                name=name, status=UpstreamStatus.OK,
                data=data["payload"], latency_ms=round(latency, 2),
            )

    @staticmethod
    def _fail(
        breaker: CircuitBreaker,
        name: str,
        status: UpstreamStatus,
        reason: str,
        start: float,
    ) -> UpstreamResult:
        breaker.record_failure()
        latency = (time.monotonic() - start) * 1000
        logger.info("upstream[%s] %s: %s", name, status.value, reason)
        return UpstreamResult(
            name=name, status=status, reason=reason,
            latency_ms=round(latency, 2),
        )
