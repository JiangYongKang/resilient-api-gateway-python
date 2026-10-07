"""聚合器。

职责：
1. 统一总时间预算：每个上游的实际等待 = min(自身超时, 预算剩余)，
   预算耗尽后未发起的调用直接记 BUDGET_EXCEEDED，慢上游不能拖垮整体；
2. 并发控制：信号量限制同时进行中的上游 HTTP 调用数；
3. 熔断闸门：调用前过熔断器，OPEN/HALF_OPEN 拒绝走降级路径，不真正发请求；
4. 部分失败合并：每个上游必有一条结果，成功携带真实数据，失败携带可区分原因；
5. 结果按配置顺序返回（与完成先后无关），并发下输出稳定可解释。
"""

from __future__ import annotations

import asyncio
import logging
import time

import httpx

from gateway.breaker import FAILURES_COUNTING_TO_BREAKER, CircuitBreakerRegistry
from gateway.config import GatewayConfig, UpstreamConfig
from gateway.models import (
    AggregateResponse,
    CircuitState,
    FailReason,
    UpstreamResult,
)
from gateway.prober import SelfHealingProber
from gateway.upstream import UpstreamCallError, UpstreamClient

logger = logging.getLogger("gateway.aggregate")

# 历史别名：计入熔断器的失败集合现在由 breaker 模块统一定义
_FAILURES_COUNTING_TO_BREAKER = FAILURES_COUNTING_TO_BREAKER

_BUDGET_GRACE = 0.25  # 外层兜底余量（秒），正常路径不会用到


class Aggregator:
    def __init__(self, config: GatewayConfig,
                 registry: CircuitBreakerRegistry,
                 client: httpx.AsyncClient | None = None) -> None:
        self.config = config
        self.registry = registry
        self._semaphore = asyncio.Semaphore(config.max_concurrent_calls)
        self._client = client
        self._client_owned = client is None
        # 后台自愈探测器：与正常流量共享同一信号量与统一预算口径
        self.prober = SelfHealingProber(
            config, registry, self._semaphore, self._get_client)

    async def aclose(self) -> None:
        await self.prober.aclose()
        if self._client_owned and self._client is not None:
            await self._client.aclose()
            self._client = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(self.config.default_upstream_timeout),
                limits=httpx.Limits(
                    max_connections=self.config.max_concurrent_calls,
                    max_keepalive_connections=self.config.max_concurrent_calls,
                ),
            )
        return self._client

    async def aggregate(self) -> AggregateResponse:
        self.prober.ensure_started()
        start = time.monotonic()
        deadline = start + self.config.total_timeout

        tasks = [
            asyncio.ensure_future(
                self._call_one(UpstreamClient(uc), deadline))
            for uc in self.config.upstreams
        ]

        results: list[UpstreamResult] = []
        timed_out_outer = False
        try:
            results = list(await asyncio.wait_for(
                asyncio.shield(asyncio.gather(*tasks)),
                timeout=self.config.total_timeout + _BUDGET_GRACE,
            ))
        except (asyncio.TimeoutError, asyncio.CancelledError):
            # 兜底：理论上每个任务都按 deadline 自我限时，走到这里说明仍有任务卡住
            timed_out_outer = True
            logger.warning("[aggregate] outer budget guard fired")
            for task, uc in zip(tasks, self.config.upstreams):
                if task.done() and not task.cancelled():
                    exc = task.exception()
                    results.append(
                        task.result() if exc is None
                        else self._internal_error_result(uc.name, exc))
                else:
                    if not task.done():
                        task.cancel()
                    results.append(self._budget_result(uc.name))

        # 保证顺序与配置一致（gather 已保证，兜底分支手工补齐后排序）
        order = {uc.name: i for i, uc in enumerate(self.config.upstreams)}
        results.sort(key=lambda r: order.get(r.upstream, 1 << 30))

        total_ms = int((time.monotonic() - start) * 1000)
        any_success = any(r.ok for r in results)
        any_budget = timed_out_outer or any(
            r.reason is FailReason.BUDGET_EXCEEDED for r in results)

        self._log_summary(results, total_ms)
        return AggregateResponse(
            success=any_success,
            all_failed=not any_success,
            total_ms=total_ms,
            timed_out=any_budget,
            results=results,
        )

    # ------------------------------------------------------------------
    async def _call_one(self, client: UpstreamClient,
                        deadline: float) -> UpstreamResult:
        uc = client.config
        breaker = self.registry.get(uc.name)
        start = time.monotonic()

        def elapsed_ms() -> int:
            return int((time.monotonic() - start) * 1000)

        # 1) 熔断闸门
        allowed, state, reject_reason = breaker.before_call()
        if not allowed:
            result = UpstreamResult(
                upstream=uc.name, ok=False, degraded=True, reason=reject_reason,
                error=(f"upstream {uc.name!r} degraded: circuit breaker is "
                       f"{state.value}"),
                elapsed_ms=elapsed_ms(), circuit_state=state)
            self._log_result(result)
            return result

        # 2) 预算闸门（可能在排队等信号量之前预算已耗尽）
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            state_after = breaker.state
            result = self._budget_result(uc.name, state_after, elapsed_ms())
            self._log_result(result)
            return result

        http_client = await self._get_client()
        try:
            # 3) 并发控制：在预算剩余时间内争取信号量
            try:
                await asyncio.wait_for(self._semaphore.acquire(),
                                       timeout=remaining)
            except asyncio.TimeoutError:
                result = self._budget_result(uc.name, breaker.state, elapsed_ms())
                self._log_result(result)
                return result

            try:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    result = self._budget_result(uc.name, breaker.state, elapsed_ms())
                    self._log_result(result)
                    return result

                limited_by_budget = remaining < uc.timeout
                try:
                    payload = await client.fetch(http_client, remaining)
                except UpstreamCallError as exc:
                    reason = exc.reason
                    # 上游是被总预算“掐断”而非自身超时：区分成预算原因
                    if (reason is FailReason.TIMEOUT and limited_by_budget):
                        reason = FailReason.BUDGET_EXCEEDED
                    new_state = breaker.record_failure(
                        counts=reason in _FAILURES_COUNTING_TO_BREAKER)
                    result = UpstreamResult(
                        upstream=uc.name, ok=False, reason=reason, error=str(exc),
                        elapsed_ms=elapsed_ms(), circuit_state=new_state)
                    self._log_result(result)
                    return result

                new_state = breaker.record_success()
                result = UpstreamResult(
                    upstream=uc.name, ok=True, data=payload.get("data"),
                    elapsed_ms=elapsed_ms(), circuit_state=new_state)
                self._log_result(result)
                return result
            finally:
                self._semaphore.release()
        except asyncio.CancelledError:
            # 外层兜底取消：慢上游按预算耗尽记账，并计入连续失败
            breaker.record_failure(counts=True)
            result = self._budget_result(uc.name, breaker.state, elapsed_ms())
            self._log_result(result)
            raise
        except Exception as exc:  # noqa: BLE001 - 任何意外都不能炸掉聚合
            new_state = breaker.record_failure(counts=False)
            result = UpstreamResult(
                upstream=uc.name, ok=False, reason=FailReason.INTERNAL_ERROR,
                error=f"gateway internal error: {exc!r}",
                elapsed_ms=elapsed_ms(), circuit_state=new_state)
            self._log_result(result)
            return result

    # ------------------------------------------------------------------
    @staticmethod
    def _budget_result(name: str, state: CircuitState = CircuitState.CLOSED,
                       elapsed_ms: int = 0) -> UpstreamResult:
        return UpstreamResult(
            upstream=name, ok=False, reason=FailReason.BUDGET_EXCEEDED,
            error=(f"upstream {name!r} not completed within the aggregate "
                   f"time budget"),
            elapsed_ms=elapsed_ms, circuit_state=state)

    @staticmethod
    def _internal_error_result(name: str, exc: BaseException) -> UpstreamResult:
        return UpstreamResult(
            upstream=name, ok=False, reason=FailReason.INTERNAL_ERROR,
            error=f"gateway internal error: {exc!r}")

    @staticmethod
    def _log_result(r: UpstreamResult) -> None:
        if r.ok:
            logger.info("[aggregate] upstream=%-12s OK   elapsed_ms=%-5d "
                        "circuit=%s data_keys=%s",
                        r.upstream, r.elapsed_ms, r.circuit_state.value,
                        sorted((r.data or {}).keys()))
        else:
            logger.info("[aggregate] upstream=%-12s FAIL reason=%-18s "
                        "degraded=%s elapsed_ms=%-5d circuit=%s error=%s",
                        r.upstream,
                        r.reason.value if r.reason else "unknown",
                        r.degraded, r.elapsed_ms, r.circuit_state.value,
                        r.error)

    @staticmethod
    def _log_summary(results: list[UpstreamResult], total_ms: int) -> None:
        ok = sum(1 for r in results if r.ok)
        fail = len(results) - ok
        states = {r.upstream: r.circuit_state.value for r in results}
        logger.info("[aggregate] DONE ok=%d failed=%d total_ms=%d circuits=%s",
                    ok, fail, total_ms, states)
