"""按上游隔离的熔断器。

状态机（每个上游独立一个实例，互不影响）：

    CLOSED --连续失败数>=阈值--> OPEN
    OPEN   --冷却到期，放行1次探测--> HALF_OPEN
    HALF_OPEN --探测成功--> CLOSED
    HALF_OPEN --探测失败--> OPEN（重新计时）
    HALF_OPEN 期间其余请求直接拒绝（CIRCUIT_REJECTED），保证同一时刻只有一次探测。

偶发单次失败只累加计数，不改变 CLOSED 状态。
before_call / record_* 均为同步方法、临界区内无 await，可在 asyncio 任务间安全调用。
"""

from __future__ import annotations

import logging
import threading
import time

from gateway.config import BreakerConfig
from gateway.models import CircuitState, FailReason

logger = logging.getLogger("gateway.breaker")

# 这些失败表示上游不健康，计入连续失败计数（聚合器与探测器共用同一口径）
FAILURES_COUNTING_TO_BREAKER = frozenset({
    FailReason.TIMEOUT,
    FailReason.BUDGET_EXCEEDED,
    FailReason.HTTP_ERROR,
    FailReason.UPSTREAM_ERROR,
    FailReason.BAD_RESPONSE,
})


class CircuitBreaker:
    def __init__(self, name: str, config: BreakerConfig) -> None:
        self.name = name
        self.config = config
        self._lock = threading.RLock()
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._opened_at = 0.0
        self._probe_in_flight = False

    # ---- 观测 ----
    @property
    def state(self) -> CircuitState:
        with self._lock:
            if self._state is CircuitState.OPEN and self._cooldown_passed():
                # 仅观测：真正的状态切换在 before_call 中完成
                return CircuitState.OPEN
            return self._state

    def snapshot(self) -> dict:
        with self._lock:
            return {
                "state": self._state.value,
                "failure_count": self._failure_count,
                "probe_in_flight": self._probe_in_flight,
            }

    def _cooldown_passed(self) -> bool:
        return time.monotonic() - self._opened_at >= self.config.recovery_timeout

    def cooldown_remaining(self) -> float:
        """距冷却结束还剩多少秒（<=0 表示已到期）。供后台探测调度使用。"""
        with self._lock:
            if self._state is not CircuitState.OPEN:
                return 0.0
            return max(0.0, self.config.recovery_timeout
                       - (time.monotonic() - self._opened_at))

    def _transition(self, new_state: CircuitState, note: str = "") -> None:
        old = self._state
        if old is new_state:
            return
        self._state = new_state
        logger.info("[breaker] upstream=%s %s -> %s %s",
                    self.name, old.value, new_state.value, note)

    # ---- 主流程 ----
    def before_call(self) -> tuple[bool, CircuitState, FailReason | None]:
        """调用前闸门。

        返回 (是否允许调用, 调用后观测到的熔断状态, 拒绝原因)。
        """
        with self._lock:
            now = time.monotonic()
            if self._state is CircuitState.CLOSED:
                return True, CircuitState.CLOSED, None

            if self._state is CircuitState.OPEN:
                if self._cooldown_passed():
                    self._transition(CircuitState.HALF_OPEN,
                                     f"(cooldown {self.config.recovery_timeout}s passed)")
                    self._probe_in_flight = True
                    return True, CircuitState.HALF_OPEN, None
                return False, CircuitState.OPEN, FailReason.CIRCUIT_OPEN

            # HALF_OPEN：探测唯一
            if not self._probe_in_flight:
                # 理论上不会走到（进入 HALF_OPEN 时立即置位），防御性处理
                self._probe_in_flight = True
                return True, CircuitState.HALF_OPEN, None
            return False, CircuitState.HALF_OPEN, FailReason.CIRCUIT_REJECTED

    def record_success(self) -> CircuitState:
        with self._lock:
            self._failure_count = 0
            self._probe_in_flight = False
            if self._state is not CircuitState.CLOSED:
                self._transition(CircuitState.CLOSED, "(probe succeeded)")
            return self._state

    def abort_probe(self) -> CircuitState:
        """探测名额已占用、但请求根本没打到上游（如信号量等待超预算）时调用。

        不算失败、不重置冷却：回到 OPEN 且保留原冷却起点，使下一次
        before_call 能立即再次放行探测；探测节奏由后台探测器的退避控制。
        """
        with self._lock:
            self._probe_in_flight = False
            if self._state is CircuitState.HALF_OPEN:
                self._transition(CircuitState.OPEN,
                                 "(probe aborted before reaching upstream)")
            return self._state

    def record_failure(self, counts: bool = True) -> CircuitState:
        """记录一次调用失败。

        counts=False 表示业务性失败（如 404），上游本身健康，不计入连续失败。
        探测中的失败一律导致重新 OPEN。
        """
        with self._lock:
            self._probe_in_flight = False
            if self._state is CircuitState.HALF_OPEN:
                self._opened_at = time.monotonic()
                self._failure_count = self.config.failure_threshold
                self._transition(CircuitState.OPEN, "(probe failed)")
                return self._state

            if counts:
                self._failure_count += 1
                if (self._state is CircuitState.CLOSED
                        and self._failure_count >= self.config.failure_threshold):
                    self._opened_at = time.monotonic()
                    self._transition(
                        CircuitState.OPEN,
                        f"(failure_count={self._failure_count} reached threshold "
                        f"{self.config.failure_threshold})",
                    )
            return self._state


class CircuitBreakerRegistry:
    """按上游名隔离管理熔断器：各上游熔断状态互不影响。"""

    def __init__(self, config: BreakerConfig) -> None:
        self.config = config
        self._breakers: dict[str, CircuitBreaker] = {}
        self._lock = threading.Lock()

    def get(self, name: str) -> CircuitBreaker:
        with self._lock:
            breaker = self._breakers.get(name)
            if breaker is None:
                breaker = CircuitBreaker(name, self.config)
                self._breakers[name] = breaker
            return breaker

    def states(self) -> dict[str, CircuitState]:
        with self._lock:
            return {name: b.state for name, b in self._breakers.items()}

    def snapshots(self) -> dict[str, dict]:
        with self._lock:
            return {name: b.snapshot() for name, b in self._breakers.items()}
