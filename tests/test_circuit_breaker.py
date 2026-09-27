from __future__ import annotations

import asyncio

from gateway.breaker import BreakerState
from gateway.models import UpstreamStatus

from .conftest import report, set_mode


async def test_breaker_opens_after_threshold_and_isolates(
    aggregator, breakers, mock_app
):
    set_mode(mock_app, "alpha", "error")
    threshold = aggregator.config.breaker_failure_threshold

    # Failures below the threshold do not trip the breaker.
    for i in range(threshold - 1):
        resp = await aggregator.aggregate()
        report(resp, breakers)
        assert resp.results["alpha"].status is UpstreamStatus.ERROR
        assert breakers["alpha"].state is BreakerState.CLOSED
        assert breakers["alpha"].consecutive_failures == i + 1

    # The failure that reaches the threshold opens the breaker.
    resp = await aggregator.aggregate()
    report(resp, breakers)
    assert resp.results["alpha"].status is UpstreamStatus.ERROR
    assert breakers["alpha"].state is BreakerState.OPEN
    assert breakers["alpha"].consecutive_failures == threshold

    # Subsequent requests fast-fail with a distinguishable reason.
    resp = await aggregator.aggregate()
    report(resp, breakers)
    assert resp.results["alpha"].status is UpstreamStatus.CIRCUIT_OPEN
    assert "circuit breaker" in resp.results["alpha"].reason

    # Isolation: other upstreams are unaffected and still serve real data.
    assert resp.results["beta"].status is UpstreamStatus.OK
    assert resp.results["gamma"].status is UpstreamStatus.OK
    assert breakers["beta"].state is BreakerState.CLOSED
    assert breakers["gamma"].state is BreakerState.CLOSED


async def test_single_failure_does_not_trip_breaker(
    aggregator, breakers, mock_app
):
    set_mode(mock_app, "beta", "error")
    await aggregator.aggregate()
    assert breakers["beta"].consecutive_failures == 1
    assert breakers["beta"].state is BreakerState.CLOSED

    # Recover before the threshold: counter resets, breaker stays closed.
    set_mode(mock_app, "beta", "ok")
    resp = await aggregator.aggregate()
    report(resp, breakers)
    assert resp.results["beta"].status is UpstreamStatus.OK
    assert breakers["beta"].consecutive_failures == 0
    assert breakers["beta"].state is BreakerState.CLOSED


async def test_breaker_half_open_probe_and_recovery(
    aggregator, breakers, mock_app
):
    set_mode(mock_app, "gamma", "error")
    for _ in range(aggregator.config.breaker_failure_threshold):
        await aggregator.aggregate()
    assert breakers["gamma"].state is BreakerState.OPEN

    # During cooldown: still fast-failing.
    resp = await aggregator.aggregate()
    assert resp.results["gamma"].status is UpstreamStatus.CIRCUIT_OPEN

    # After cooldown: half-open probe is allowed through; upstream healed.
    await asyncio.sleep(aggregator.config.breaker_cooldown_s + 0.05)
    set_mode(mock_app, "gamma", "ok")
    resp = await aggregator.aggregate()
    report(resp, breakers)
    assert resp.results["gamma"].status is UpstreamStatus.OK
    assert breakers["gamma"].state is BreakerState.CLOSED
    assert breakers["gamma"].consecutive_failures == 0


async def test_half_open_probe_failure_reopens(aggregator, breakers, mock_app):
    set_mode(mock_app, "gamma", "error")
    for _ in range(aggregator.config.breaker_failure_threshold):
        await aggregator.aggregate()
    assert breakers["gamma"].state is BreakerState.OPEN

    # Cooldown passes but upstream is still broken: probe fails, re-opens.
    await asyncio.sleep(aggregator.config.breaker_cooldown_s + 0.05)
    resp = await aggregator.aggregate()
    report(resp, breakers)
    assert resp.results["gamma"].status is UpstreamStatus.ERROR
    assert breakers["gamma"].state is BreakerState.OPEN

    # Immediately after re-opening: fast fail again.
    resp = await aggregator.aggregate()
    assert resp.results["gamma"].status is UpstreamStatus.CIRCUIT_OPEN
