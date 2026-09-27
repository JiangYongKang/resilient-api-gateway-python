from __future__ import annotations

import asyncio
import time

from gateway.models import UpstreamStatus

from .conftest import NAMES, report, set_mode


async def test_concurrent_burst_stable_and_bounded(
    aggregator, breakers, mock_app
):
    # Give every upstream a small latency so requests genuinely overlap.
    for name in NAMES:
        set_mode(mock_app, name, "ok", delay_s=0.05)

    burst = 20
    start = time.monotonic()
    responses = await asyncio.gather(
        *(aggregator.aggregate() for _ in range(burst))
    )
    elapsed = time.monotonic() - start
    report(responses[0], breakers)

    # Every request succeeds with identical, deterministic content.
    first = responses[0]
    for resp in responses:
        assert resp.failed == []
        assert sorted(resp.succeeded) == sorted(NAMES)
        for name in NAMES:
            assert resp.results[name].status is UpstreamStatus.OK
            assert resp.results[name].data == first.results[name].data

    # Each upstream received exactly one request per aggregate call.
    limit = aggregator.config.max_concurrent_per_upstream
    for name in NAMES:
        assert mock_app.state.counts[name] == burst
        observed = mock_app.state.max_inflight[name]
        print(f"  upstream={name} max_concurrent_observed={observed} limit={limit}")
        assert 1 < observed <= limit

    # Bounded concurrency means the burst takes roughly burst/limit rounds,
    # and is far from unbounded-parallel or fully-serial timing.
    assert elapsed < burst * 0.05


async def test_repeat_requests_deterministic(aggregator, breakers, mock_app):
    set_mode(mock_app, "beta", "error")
    snapshots = []
    for _ in range(3):
        resp = await aggregator.aggregate()
        report(resp, breakers)
        snapshots.append(
            {n: (r.status.value, r.reason) for n, r in resp.results.items()}
        )
    # Same conditions -> same per-upstream statuses and reasons every time.
    assert snapshots[0] == snapshots[1] == snapshots[2]
