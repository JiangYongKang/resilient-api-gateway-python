"""Per-upstream circuit breaker."""
from __future__ import annotations

import enum
import logging
import time

logger = logging.getLogger("gateway.breaker")


class BreakerState(str, enum.Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    """Tracks consecutive failures for one upstream.

    - CLOSED: requests flow; consecutive failures are counted.
    - OPEN: after ``failure_threshold`` consecutive failures, requests are
      rejected immediately (fast fail) for ``cooldown_s`` seconds.
    - HALF_OPEN: after the cooldown, a single probe request is allowed;
      success closes the breaker, failure re-opens it.
    """

    def __init__(self, name: str, failure_threshold: int, cooldown_s: float) -> None:
        self.name = name
        self.failure_threshold = failure_threshold
        self.cooldown_s = cooldown_s
        self._state = BreakerState.CLOSED
        self._consecutive_failures = 0
        self._opened_at: float | None = None
        self._half_open_in_flight = False

    @property
    def state(self) -> BreakerState:
        if self._state is BreakerState.OPEN and self._opened_at is not None:
            if time.monotonic() - self._opened_at >= self.cooldown_s:
                self._transition(BreakerState.HALF_OPEN)
        return self._state

    @property
    def consecutive_failures(self) -> int:
        return self._consecutive_failures

    def allow_request(self) -> bool:
        """Return True if a request may be sent to the upstream now."""
        state = self.state  # may transition OPEN -> HALF_OPEN
        if state is BreakerState.CLOSED:
            return True
        if state is BreakerState.OPEN:
            return False
        # HALF_OPEN: allow exactly one probe at a time.
        if self._half_open_in_flight:
            return False
        self._half_open_in_flight = True
        return True

    def record_success(self) -> None:
        self._half_open_in_flight = False
        if self._state is not BreakerState.CLOSED:
            self._transition(BreakerState.CLOSED)
        self._consecutive_failures = 0

    def record_failure(self) -> None:
        self._half_open_in_flight = False
        self._consecutive_failures += 1
        if self._state is BreakerState.HALF_OPEN:
            self._transition(BreakerState.OPEN)
        elif (
            self._state is BreakerState.CLOSED
            and self._consecutive_failures >= self.failure_threshold
        ):
            self._transition(BreakerState.OPEN)

    def _transition(self, new_state: BreakerState) -> None:
        old = self._state
        self._state = new_state
        if new_state is BreakerState.OPEN:
            self._opened_at = time.monotonic()
        logger.info(
            "breaker[%s] %s -> %s (consecutive_failures=%d)",
            self.name, old.value, new_state.value, self._consecutive_failures,
        )
