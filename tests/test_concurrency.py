"""并发控制与突发流量测试。

覆盖：
- 信号量对同一上游的在飞请求数生效（突发 N 个聚合，单个 mock 观测到的
  并发峰值不超过 max_concurrent_calls）；
- 同时到达的突发请求结果稳定、确定、可解释：结构一致、成功集合一致、
  数据来自真实上游（相同 mock 的 value 固定）；
- 小并发预算（=1）时请求被排队限流但不丢结果、不超总预算；
- 突发期间熔断器状态转换确定：持续错误上游在达到阈值后稳定 OPEN。
"""

from __future__ import annotations

import asyncio

import pytest

from tests.conftest import DEFAULT_UPSTREAMS, print_report


@pytest.mark.asyncio
async def test_concurrency_cap_limits_in_flight_upstream_calls(
        gateway_factory, mock_cluster):
    for name in DEFAULT_UPSTREAMS:
        mock_cluster.set_mode(name, "slow", delay=0.25)

    cap = 2
    gw = await gateway_factory(
        total_timeout=5.0, upstream_timeout=2.0, max_concurrency=cap)

    burst = 5
    responses = await asyncio.gather(
        *[gw.aggregate() for _ in range(burst)])
    bodies = [r.json() for r in responses]
    print_report(f"concurrency cap={cap}, burst={burst}", bodies[0],
                 await gw.circuits())

    # 每次聚合 3 个结果，没有丢失；慢上游全部成功
    assert all(len(b["results"]) == 3 for b in bodies)
    assert all(r["ok"] for b in bodies for r in b["results"])

    # 单个上游在整个突发期间观测到的服务端并发峰值不得超过信号量上限
    for name in DEFAULT_UPSTREAMS:
        peak = mock_cluster.mode_info(name)["in_flight_peak"]
        assert peak <= cap, (name, peak)
    # cap=2 且 burst=5 必然有过并行（否则限流没生效的判据无意义）
    assert any(mock_cluster.mode_info(n)["in_flight_peak"] >= 1
               for n in DEFAULT_UPSTREAMS)


@pytest.mark.asyncio
async def test_burst_responses_are_stable_and_deterministic(
        gateway_factory, mock_cluster):
    # users/payments 正常，orders 返回 404（业务失败、不改熔断状态）
    mock_cluster.set_mode("users", "ok")
    mock_cluster.set_mode("orders", "not_found")
    mock_cluster.set_mode("payments", "ok")

    # 阈值设得很高，保证突发期间不发生熔断 -> 每次结果应完全一致
    gw = await gateway_factory(failure_threshold=100, recovery_timeout=30)

    burst = 12
    responses = await asyncio.gather(
        *[gw.aggregate() for _ in range(burst)])
    bodies = [r.json() for r in responses]

    print_report(f"deterministic burst={burst}", bodies[0], await gw.circuits())

    signatures = set()
    for body in bodies:
        assert body["success"] is True
        assert body["all_failed"] is False
        assert [r["upstream"] for r in body["results"]] == list(
            DEFAULT_UPSTREAMS)
        sig = tuple(
            (r["upstream"], r["ok"], r["reason"],
             None if r["data"] is None else tuple(sorted(r["data"].items())))
            for r in body["results"]
        )
        signatures.add(sig)

    # 所有突发响应语义完全一致（只有一种结果签名）
    assert len(signatures) == 1, signatures
    # 成功数据是真实 mock 数据，不是伪造补齐
    sig = next(iter(signatures))
    data_map = {upstream: (ok, reason, data)
                for upstream, ok, reason, data in sig}
    assert data_map["users"][2] == (("value", "users"),)
    assert data_map["payments"][2] == (("value", "payments"),)
    assert data_map["orders"][1] == "upstream_not_found"
    assert data_map["orders"][2] is None


@pytest.mark.asyncio
async def test_serialized_concurrency_does_not_drop_results_or_exceed_budget(
        gateway_factory, mock_cluster):
    for name in DEFAULT_UPSTREAMS:
        mock_cluster.set_mode(name, "slow", delay=0.15)

    # max_concurrency=1：3 个上游在每次聚合内排队执行
    gw = await gateway_factory(total_timeout=5.0, upstream_timeout=1.0,
                               max_concurrency=1)

    responses = await asyncio.gather(*[gw.aggregate() for _ in range(3)])
    bodies = [r.json() for r in responses]
    print_report("serialized concurrency (cap=1) x3", bodies[0],
                 await gw.circuits())

    for body in bodies:
        assert body["success"] is True
        assert len(body["results"]) == 3
        assert all(r["ok"] for r in body["results"])
        assert all(r["data"] is not None for r in body["results"])


@pytest.mark.asyncio
async def test_burst_under_continuous_failure_opens_breaker_deterministically(
        gateway_factory, mock_cluster):
    mock_cluster.set_mode("users", "ok")
    mock_cluster.set_mode("orders", "ok")
    mock_cluster.set_mode("payments", "error")  # 持续失败

    gw = await gateway_factory(failure_threshold=3, recovery_timeout=30)

    # 串行 3 次把 payments 打到 OPEN（避免对连续失败计数的竞态）
    for i in range(3):
        body = (await gw.aggregate()).json()
    print_report("breaker warm-up: 3 failures", body, await gw.circuits())
    assert (await gw.circuits())["states"]["payments"] == "OPEN"

    # 熔断后突发 10 个并发聚合：payments 必须全部稳定降级，users/orders 全部成功
    responses = await asyncio.gather(*[gw.aggregate() for _ in range(10)])
    bodies = [r.json() for r in responses]
    print_report("burst after OPEN (10 concurrent)", bodies[0],
                 await gw.circuits())

    for body in bodies:
        by = {r["upstream"]: r for r in body["results"]}
        assert by["users"]["ok"] and by["orders"]["ok"]
        p = by["payments"]
        assert p["ok"] is False
        assert p["reason"] == "circuit_open"
        assert p["degraded"] is True
        assert p["circuit_state"] == "OPEN"

    assert (await gw.circuits())["states"]["payments"] == "OPEN"
