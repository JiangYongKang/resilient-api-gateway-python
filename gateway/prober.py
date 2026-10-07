"""后台自愈探测器：熔断冷却后主动发探测请求，不等客户端流量触发。

工作方式：
- 一个后台 asyncio 任务按固定间隔扫描所有上游的熔断器；
- 发现 OPEN 且冷却到期、退避窗口也到期的上游，就主动发起一次真实探测；
- 探测复用与正常聚合完全相同的约束：统一时间预算（probe.timeout）、
  全局信号量（与正常流量共享，等不到就让出、下轮再来，不挤占健康上游容量）；
- 同一上游同一时刻最多一次探测在飞（熔断器 HALF_OPEN 单探测语义保证）；
- 探测真正打到上游并失败 -> 指数退避（有上限），避免把预算持续耗在坏上游上；
- 探测没打到上游（信号量等待超预算）-> 不算失败、不重置冷却，下轮立即重试，
  保证并发打满/预算紧张时探测最终一定能真正到达上游（收敛性）；
- 探测永不彻底停止：连续失败次数只用于让退避饱和到 max_backoff，
  上游一旦恢复，下一次探测就会把它拉回 CLOSED。

关键状态流转都通过 gateway.prober logger 按时间顺序输出。
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field

from gateway.breaker import (
    FAILURES_COUNTING_TO_BREAKER,
    CircuitBreakerRegistry,
)
from gateway.config import GatewayConfig, UpstreamConfig
from gateway.models import CircuitState
from gateway.upstream import UpstreamCallError, UpstreamClient

logger = logging.getLogger("gateway.prober")

# 探测中止（没打到上游）后的最快重试间隔下界，避免扫描间隔被调成 0 时热循环
_MIN_RETRY_AFTER_ABORT = 0.05


@dataclass
class ProbeStats:
    """单个上游的恢复进展（可观测）。"""

    attempts: int = 0            # 连续“真实探测失败”次数（用于退避）
    total_probes: int = 0        # 真正打到上游的探测总数
    in_flight: bool = False      # 当前是否有探测在飞
    last_result: str | None = None  # "success" / "failure" / "aborted"
    last_probe_at: float = 0.0   # 最近一次真实探测完成时刻（monotonic）
    next_probe_not_before: float = 0.0  # 退避窗口：该时刻之前不再发起探测


@dataclass
class SelfHealingProber:
    config: GatewayConfig
    registry: CircuitBreakerRegistry
    semaphore: asyncio.Semaphore           # 与聚合器共享的全局并发信号量
    client_getter: object                  # Callable[[], Awaitable[httpx.AsyncClient]]
    _stats: dict[str, ProbeStats] = field(default_factory=dict)
    _task: asyncio.Task | None = None
    _probe_tasks: set[asyncio.Task] = field(default_factory=set)

    # ---- 生命周期 ----
    def ensure_started(self) -> None:
        """在运行中的事件循环里惰性启动后台扫描任务（幂等）。"""
        if not self.config.probe.enabled:
            return
        if self._task is None or self._task.done():
            self._task = asyncio.ensure_future(self._run())
            logger.info("[prober] started interval=%.3fs timeout=%.3fs "
                        "backoff=%.3fs..%.3fs max_attempts=%d",
                        self.config.probe.interval, self.config.probe.timeout,
                        self.config.probe.base_backoff,
                        self.config.probe.max_backoff,
                        self.config.probe.max_attempts)

    async def aclose(self) -> None:
        task, self._task = self._task, None
        if task is not None and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        probes, self._probe_tasks = self._probe_tasks, set()
        for probe in probes:
            probe.cancel()
        for probe in probes:
            try:
                await probe
            except asyncio.CancelledError:
                pass
        if task is not None or probes:
            logger.info("[prober] stopped")

    # ---- 观测 ----
    def _stat(self, name: str) -> ProbeStats:
        stat = self._stats.get(name)
        if stat is None:
            stat = self._stats[name] = ProbeStats()
        return stat

    def snapshot(self) -> dict:
        now = time.monotonic()
        upstreams: dict[str, dict] = {}
        for uc in self.config.upstreams:
            breaker = self.registry.get(uc.name)
            stat = self._stat(uc.name)
            state = breaker.state
            upstreams[uc.name] = {
                "state": state.value,
                # 已恢复到可以被正常调用
                "recovered": state is CircuitState.CLOSED,
                "probe_in_flight": stat.in_flight,
                "attempts": stat.attempts,
                "total_probes": stat.total_probes,
                "last_result": stat.last_result,
                "last_probe_ago_ms": (
                    int((now - stat.last_probe_at) * 1000)
                    if stat.last_probe_at else None),
                "next_probe_in_ms": (
                    max(0, int((stat.next_probe_not_before - now) * 1000))
                    if state is CircuitState.OPEN else 0),
                "cooldown_remaining_ms": int(breaker.cooldown_remaining() * 1000),
            }
        p = self.config.probe
        return {
            "enabled": p.enabled,
            "config": {
                "interval": p.interval,
                "timeout": p.timeout,
                "base_backoff": p.base_backoff,
                "max_backoff": p.max_backoff,
                "max_attempts": p.max_attempts,
            },
            "upstreams": upstreams,
        }

    # ---- 主循环 ----
    async def _run(self) -> None:
        interval = max(self.config.probe.interval, _MIN_RETRY_AFTER_ABORT)
        while True:
            await asyncio.sleep(interval)
            try:
                self._scan()
            except Exception:  # 扫描异常不能杀死探测循环
                logger.exception("[prober] scan failed")

    def _scan(self) -> None:
        now = time.monotonic()
        for uc in self.config.upstreams:
            breaker = self.registry.get(uc.name)
            if breaker.state is not CircuitState.OPEN:
                continue
            if breaker.cooldown_remaining() > 0:
                continue
            stat = self._stat(uc.name)
            if stat.in_flight or now < stat.next_probe_not_before:
                continue
            stat.in_flight = True
            task = asyncio.ensure_future(self._probe_once(uc))
            self._probe_tasks.add(task)
            task.add_done_callback(self._probe_tasks.discard)

    # ---- 单次探测 ----
    def _backoff(self, attempts: int) -> float:
        p = self.config.probe
        exp = max(0, min(attempts, p.max_attempts) - 1)
        return min(p.base_backoff * (2 ** exp), p.max_backoff)

    async def _probe_once(self, uc: UpstreamConfig) -> None:
        breaker = self.registry.get(uc.name)
        stat = self._stat(uc.name)
        try:
            allowed, state, _ = breaker.before_call()
            if not allowed or state is not CircuitState.HALF_OPEN:
                # 已被正常流量/其他探测抢先，或上游已恢复：无需探测
                return

            logger.info("[prober] upstream=%s probe START (cooldown passed, "
                        "attempts=%d)", uc.name, stat.attempts)
            deadline = time.monotonic() + self.config.probe.timeout

            # 预算内等全局信号量；等不到就让出，不算失败、下轮再来
            try:
                await asyncio.wait_for(
                    self._semaphore_acquire(), timeout=max(0.0, deadline - time.monotonic()))
            except TimeoutError:
                breaker.abort_probe()
                stat.last_result = "aborted"
                stat.next_probe_not_before = (
                    time.monotonic() + max(self.config.probe.interval,
                                           _MIN_RETRY_AFTER_ABORT))
                logger.info("[prober] upstream=%s probe ABORTED: no concurrency "
                            "slot within probe budget; will retry", uc.name)
                return

            try:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    breaker.abort_probe()
                    stat.last_result = "aborted"
                    logger.info("[prober] upstream=%s probe ABORTED: probe "
                                "budget exhausted before request", uc.name)
                    return

                http_client = await self.client_getter()
                try:
                    await UpstreamClient(uc).fetch(http_client, remaining)
                except UpstreamCallError as exc:
                    new_state = breaker.record_failure(
                        counts=exc.reason in FAILURES_COUNTING_TO_BREAKER)
                    stat.attempts += 1
                    stat.total_probes += 1
                    stat.last_result = "failure"
                    stat.last_probe_at = time.monotonic()
                    backoff = self._backoff(stat.attempts)
                    stat.next_probe_not_before = stat.last_probe_at + backoff
                    logger.info("[prober] upstream=%s probe FAILED reason=%s "
                                "attempts=%d backoff=%.3fs circuit=%s",
                                uc.name, exc.reason.value, stat.attempts,
                                backoff, new_state.value)
                    return

                new_state = breaker.record_success()
                stat.attempts = 0
                stat.total_probes += 1
                stat.last_result = "success"
                stat.last_probe_at = time.monotonic()
                stat.next_probe_not_before = 0.0
                logger.info("[prober] upstream=%s probe OK -> recovered "
                            "circuit=%s total_probes=%d",
                            uc.name, new_state.value, stat.total_probes)
            finally:
                self._semaphore_release()
        except asyncio.CancelledError:
            breaker.abort_probe()
            raise
        except Exception:  # 单次探测异常不能杀死探测循环
            breaker.abort_probe()
            logger.exception("[prober] upstream=%s probe crashed", uc.name)
        finally:
            stat.in_flight = False

    # 信号量获取/释放包一层，便于测试替换与阅读
    async def _semaphore_acquire(self) -> None:
        await self.semaphore.acquire()

    def _semaphore_release(self) -> None:
        self.semaphore.release()
