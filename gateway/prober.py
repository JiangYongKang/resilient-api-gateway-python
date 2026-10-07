"""自愈探测器：上游熔断后的后台主动恢复机制。

工作方式：
- 后台循环按 interval 扫描所有上游的熔断器；
- 发现 OPEN 且冷却到期的熔断器，主动发起一次真实探测请求
  （不等下一个客户端请求被动触发 HALF_OPEN）；
- 探测与正常流量共用同一个并发信号量（不挤占健康上游之外的额外容量），
  且有自身独立的时间预算；同一上游同一时刻至多一次探测在飞
  （由熔断器 HALF_OPEN 唯一探测位保证）；
- 探测持续失败时按指数退避节流（封顶 max_backoff），
  不把预算一直烧在长期恢复不了的上游上；
- 收敛性：探测任务一旦启动，会在 acquire_timeout 内耐心排队等并发槽位，
  信号量按 FIFO 唤醒，压力再大也终将拿到槽位真正打到上游；
  实在等不到则中止本次探测（不计上游失败），下个周期重试。

恢复进展通过 snapshots() 只读观测；关键状态流转都写日志（按时间顺序）。
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from gateway.breaker import CircuitBreakerRegistry
from gateway.config import ProbeConfig, UpstreamConfig

logger = logging.getLogger("gateway.prober")

# 探测执行函数：入参上游名，返回 ProbeOutcome
ProbeFn = Callable[[str], Awaitable["ProbeOutcome"]]


@dataclass
class ProbeOutcome:
    """一次探测调度的结果。

    attempted=False 表示探测未能真正打到上游（如等不到并发槽位被中止），
    不计入上游失败，也不触发退避升级。
    """

    attempted: bool
    ok: bool = False
    reason: str | None = None
    error: str | None = None


@dataclass
class ProbeStats:
    """单个上游的探测进展（只读观测用）。"""

    attempts: int = 0               # 真实打到上游的探测次数
    successes: int = 0
    failures: int = 0
    consecutive_failures: int = 0   # 用于退避
    last_probe_at: float | None = None   # monotonic 时间戳
    last_result: str = "none"       # none / success / failure
    last_error: str | None = None
    next_probe_at: float | None = None   # 退避后的下次允许探测时间
    in_flight: bool = False

    def snapshot(self, recovered: bool) -> dict:
        return {
            "attempts": self.attempts,
            "successes": self.successes,
            "failures": self.failures,
            "consecutive_failures": self.consecutive_failures,
            "last_result": self.last_result,
            "last_error": self.last_error,
            "in_flight": self.in_flight,
            "recovered": recovered,
            "next_probe_in_ms": (
                None if self.next_probe_at is None
                else max(0, int((self.next_probe_at - time.monotonic()) * 1000))
            ),
        }


class RecoveryProber:
    """后台自愈探测循环。"""

    def __init__(self, config: ProbeConfig,
                 registry: CircuitBreakerRegistry,
                 upstreams: tuple[UpstreamConfig, ...],
                 probe_fn: ProbeFn) -> None:
        self.config = config
        self.registry = registry
        self.upstreams = upstreams
        self._probe_fn = probe_fn
        self._stats: dict[str, ProbeStats] = {
            uc.name: ProbeStats() for uc in upstreams}
        self._task: asyncio.Task | None = None
        self._probe_sem = asyncio.Semaphore(config.max_concurrent_probes)
        self._pending: set[str] = set()   # 已调度未完成的探测，防重复

    @property
    def started(self) -> bool:
        return self._task is not None and not self._task.done()

    # ------------------------------------------------------------------
    def start(self) -> None:
        """启动后台探测循环（幂等；需在运行中的事件循环里调用）。"""
        if not self.config.enabled or self.started:
            return
        self._task = asyncio.ensure_future(self._run())
        logger.info("[prober] started interval=%.2fs timeout=%.2fs "
                    "max_concurrent_probes=%d max_backoff=%.1fs",
                    self.config.interval, self.config.timeout,
                    self.config.max_concurrent_probes, self.config.max_backoff)

    async def stop(self) -> None:
        """停止后台探测循环并等待退出。"""
        task, self._task = self._task, None
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        logger.info("[prober] stopped")

    # ------------------------------------------------------------------
    def snapshots(self) -> dict[str, dict]:
        """各上游探测进展的只读快照。"""
        states = self.registry.states()
        return {
            name: stats.snapshot(
                recovered=states.get(name) is not None
                and states[name].value == "CLOSED")
            for name, stats in self._stats.items()
        }

    # ------------------------------------------------------------------
    async def _run(self) -> None:
        while True:
            await asyncio.sleep(self.config.interval)
            try:
                self._tick()
            except Exception:  # noqa: BLE001 - 扫描异常不能杀死探测循环
                logger.exception("[prober] tick failed")

    def _tick(self) -> None:
        now = time.monotonic()
        for uc in self.upstreams:
            name = uc.name
            if name in self._pending:
                continue
            stats = self._stats[name]
            if stats.in_flight:
                continue
            if not self.registry.get(name).probe_due():
                continue
            if stats.next_probe_at is not None and now < stats.next_probe_at:
                continue  # 退避中：长期失败的上游不频繁烧预算
            self._pending.add(name)
            asyncio.ensure_future(self._probe(name))

    async def _probe(self, name: str) -> None:
        try:
            # 限制同时在飞的探测总数，保护健康上游容量
            async with self._probe_sem:
                stats = self._stats[name]
                stats.in_flight = True
                logger.info("[prober] probing upstream=%s (attempt #%d)",
                            name, stats.attempts + 1)
                try:
                    outcome = await self._probe_fn(name)
                except Exception as exc:  # noqa: BLE001 - 探测失败不能炸掉循环
                    outcome = ProbeOutcome(attempted=False,
                                           error=f"prober internal: {exc!r}")
                finally:
                    stats.in_flight = False
            self._record(name, outcome)
        finally:
            self._pending.discard(name)

    def _record(self, name: str, outcome: ProbeOutcome) -> None:
        stats = self._stats[name]
        now = time.monotonic()
        if not outcome.attempted:
            # 没真正打到上游：不计失败、不升级退避，下个周期重试
            logger.info("[prober] probe upstream=%s aborted before reaching "
                        "upstream: %s", name, outcome.error)
            return

        stats.attempts += 1
        stats.last_probe_at = now
        if outcome.ok:
            stats.successes += 1
            stats.consecutive_failures = 0
            stats.last_result = "success"
            stats.last_error = None
            stats.next_probe_at = None
            logger.info("[prober] probe upstream=%s SUCCEEDED -> recovered",
                        name)
            return

        stats.failures += 1
        stats.consecutive_failures += 1
        stats.last_result = "failure"
        stats.last_error = outcome.error
        backoff = min(
            self.config.max_backoff,
            self.config.interval
            * self.config.backoff_multiplier ** stats.consecutive_failures,
        )
        stats.next_probe_at = now + backoff
        logger.info("[prober] probe upstream=%s FAILED reason=%s "
                    "consecutive_failures=%d next_probe_in=%.2fs",
                    name, outcome.reason, stats.consecutive_failures, backoff)
