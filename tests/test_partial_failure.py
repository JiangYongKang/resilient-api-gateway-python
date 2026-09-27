from __future__ import annotations

import time

import httpx

from gateway.models import UpstreamStatus

from .conftest import report, set_mode


async def test_all_upstreams_ok(aggregator, breakers, mock_app):
    resp = await aggregator.aggregate()
    report(resp, breakers)
    assert resp.failed == []
    assert sorted(resp.succeeded) == ["alpha", "beta", "gamma"]
    for name, r in resp.results.items():
        assert r.status is UpstreamStatus.OK
        assert r.data == {"upstream": name, "value": f"hello-from-{name}"}


async def test_partial_failure_error_and_malformed(aggregator, breakers, mock_app):
    set_mode(mock_app, "beta", "error")
    set_mode(mock_app, "gamma", "malformed")
    resp = await aggregator.aggregate()
    report(resp, breakers)

    # The healthy upstream still contributes its real data.
    assert resp.results["alpha"].status is UpstreamStatus.OK
    assert resp.results["alpha"].data["upstream"] == "alpha"

    # Failures are explicit, with distinguishable reasons - not silently dropped.
    assert resp.results["beta"].status is UpstreamStatus.ERROR
    assert "503" in resp.results["beta"].reason
    assert resp.results["beta"].data is None
    assert resp.results["gamma"].status is UpstreamStatus.MALFORMED
    assert "payload" in resp.results["gamma"].reason
    assert resp.results["gamma"].data is None

    assert resp.succeeded == ["alpha"]
    assert resp.failed == ["beta", "gamma"]


async def test_slow_upstream_cancelled_at_total_budget(
    aggregator, breakers, mock_app
):
    set_mode(mock_app, "gamma", "slow", delay_s=5.0)
    start = time.monotonic()
    resp = await aggregator.aggregate()
    elapsed = time.monotonic() - start
    report(resp, breakers)

    # Returns at the total budget (0.8s), far before the 5s upstream delay.
    assert elapsed < 2.0
    assert resp.results["gamma"].status is UpstreamStatus.TIMEOUT
    assert "budget" in resp.results["gamma"].reason
    assert resp.results["gamma"].data is None
    assert resp.succeeded == ["alpha", "beta"]


async def test_per_request_timeout_classified(aggregator, breakers, monkeypatch):
    async def raise_timeout(*args, **kwargs):
        raise httpx.ReadTimeout("simulated read timeout")

    monkeypatch.setattr(aggregator.client, "get", raise_timeout)
    resp = await aggregator.aggregate()
    report(resp, breakers)
    assert resp.failed == ["alpha", "beta", "gamma"]
    for r in resp.results.values():
        assert r.status is UpstreamStatus.TIMEOUT
        assert "no response within" in r.reason
