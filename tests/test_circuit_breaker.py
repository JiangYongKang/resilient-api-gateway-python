"""熔断触发、隔离、半开探测恢复测试。

覆盖：
- 偶发单次/两次失败不熔断；达到阈值才 OPEN，且 OPEN 后快速失败原因可区分；
- 各上游熔断状态相互隔离；
- 冷却后 HALF_OPEN 只放一次探测：探测成功 -> CLOSED 恢复；探测失败 -> 重新 OPEN；
- 恢复后的上游能再次返回真实数据（成功计数清零，不被旧失败拖垮）。
"""

from __future__ import annotations

import asyncio

import pytest

from tests.conftest import print_report


async def _one(gw, label):
    resp = await gw.aggregate()
    body = resp.json()
    print_report(label, body, await gw.circuits())
    return {r["upstream"]: r for r in body["results"]}, body


@pytest.mark.asyncio
async def test_single_and_double_failures_do_not_trip(gateway_factory,
                                                       mock_cluster):
    # orders 恰好失败 2 次（阈值 3），第 3 次恢复正常
    mock_cluster.set_mode("orders", "sequence", sequence=["error", "error", "ok"])
    gw = await gateway_factory(failure_threshold=3, recovery_timeout=30)

    results, _ = await _one(gw, "breaker: orders failure #1")
    assert results["orders"]["reason"] == "http_error"
    assert results["orders"]["circuit_state"] == "CLOSED"
    assert results["orders"]["degraded"] is False  # 真的请求了上游

    results, _ = await _one(gw, "breaker: orders failure #2")
    assert results["orders"]["circuit_state"] == "CLOSED"
    assert results["orders"]["degraded"] is False  # 两次失败仍放行

    results, _ = await _one(gw, "breaker: orders recovered #3")
    assert results["orders"]["ok"] is True
    assert results["orders"]["circuit_state"] == "CLOSED"

    states = (await gw.circuits())["states"]
    assert states == {"users": "CLOSED", "orders": "CLOSED", "payments": "CLOSED"}


@pytest.mark.asyncio
async def test_threshold_opens_breaker_then_fast_fail_with_distinct_reason(
        gateway_factory, mock_cluster):
    mock_cluster.set_mode("orders", "error")
    gw = await gateway_factory(failure_threshold=3, recovery_timeout=30)

    for i in range(1, 4):
        results, _ = await _one(gw, f"breaker: orders failing #{i}")
    assert results["orders"]["circuit_state"] == "OPEN"

    # 第 4 次起：熔断打开，快速失败，不真正打上游
    results, _ = await _one(gw, "breaker: orders OPEN -> degraded")
    o = results["orders"]
    assert o["ok"] is False
    assert o["reason"] == "circuit_open"
    assert o["degraded"] is True
    assert o["elapsed_ms"] < 20  # 快速失败
    assert o["circuit_state"] == "OPEN"


@pytest.mark.asyncio
async def test_breaker_states_are_isolated_between_upstreams(
        gateway_factory, mock_cluster):
    # payments 持续失败直到熔断；orders/users 始终正常
    mock_cluster.set_mode("payments", "error")
    gw = await gateway_factory(failure_threshold=2, recovery_timeout=30)

    for i in range(1, 3):
        results, _ = await _one(gw, f"isolation: payments failing #{i}")
    results, _ = await _one(gw, "isolation: payments OPEN, others healthy")

    assert results["payments"]["reason"] == "circuit_open"
    assert results["users"]["ok"] is True
    assert results["orders"]["ok"] is True
    states = (await gw.circuits())["states"]
    assert states["payments"] == "OPEN"
    assert states["users"] == "CLOSED"
    assert states["orders"] == "CLOSED"


@pytest.mark.asyncio
async def test_half_open_probe_success_recovers(gateway_factory, mock_cluster):
    # 前 3 次失败 -> OPEN；冷却后再请求时，mock 已恢复 -> 探测成功 -> CLOSED
    mock_cluster.set_mode("orders", "error")
    gw = await gateway_factory(failure_threshold=3, recovery_timeout=0.3)

    for i in range(1, 4):
        await _one(gw, f"recover: failing #{i}")
    assert (await gw.circuits())["states"]["orders"] == "OPEN"

    await asyncio.sleep(0.45)  # 等冷却结束

    # 第 4 次聚合：orders 作为 HALF_OPEN 探测，应当真实请求并成功
    mock_cluster.set_mode("orders", "ok")
    results, _ = await _one(gw, "recover: half-open probe succeeds")
    assert results["orders"]["ok"] is True
    assert results["orders"]["degraded"] is False
    assert results["orders"]["circuit_state"] == "CLOSED"
    assert results["orders"]["data"] == {"value": "orders"}

    # 再一次确认完全恢复
    results, _ = await _one(gw, "recover: steady after recovery")
    assert results["orders"]["ok"] is True
    assert (await gw.circuits())["states"]["orders"] == "CLOSED"


@pytest.mark.asyncio
async def test_half_open_probe_failure_reopens(gateway_factory, mock_cluster):
    mock_cluster.set_mode("orders", "error")
    gw = await gateway_factory(failure_threshold=2, recovery_timeout=0.3)

    for i in range(1, 3):
        await _one(gw, f"reopen: failing #{i}")
    assert (await gw.circuits())["states"]["orders"] == "OPEN"

    await asyncio.sleep(0.45)
    # 冷却后探测，但上游仍在失败 -> 回到 OPEN（冷却重新计时）
    results, _ = await _one(gw, "reopen: half-open probe fails")
    assert results["orders"]["ok"] is False
    assert results["orders"]["reason"] == "http_error"  # 探测是真实请求
    assert results["orders"]["circuit_state"] == "OPEN"

    # 紧接着的下一次仍然快速失败（新冷却期未到）
    results, _ = await _one(gw, "reopen: still OPEN after failed probe")
    assert results["orders"]["reason"] == "circuit_open"
    assert results["orders"]["degraded"] is True


@pytest.mark.asyncio
async def test_only_one_in_flight_probe_in_half_open(gateway_factory,
                                                      mock_cluster):
    # 冷却到期后并发发起多个聚合请求：HALF_OPEN 只允许一个探测，
    # 其余并发请求得到 circuit_rejected，原因可区分、结果确定。
    mock_cluster.set_mode("orders", "slow")
    mock_cluster.set_mode("users", "ok")
    mock_cluster.set_mode("payments", "ok")
    gw = await gateway_factory(
        failure_threshold=1, recovery_timeout=0.3,
        upstream_timeout=2.0, total_timeout=3.0)

    # 先打一次让 orders（slow 仍算成功）不行——slow 是 200。
    # 改为先 error 打熔断，再在探测窗口用 slow 占住探测。
    mock_cluster.set_mode("orders", "error")
    await _one(gw, "probe-guard: open breaker")
    assert (await gw.circuits())["states"]["orders"] == "OPEN"

    await asyncio.sleep(0.45)
    # 探测请求会落到 slow 上（给足时间让它在飞），随后并发再来两个请求
    mock_cluster.set_mode("orders", "slow", delay=1.0)

    async def call():
        return (await gw.aggregate()).json()

    probe = asyncio.ensure_future(call())
    await asyncio.sleep(0.15)  # 确保探测已进入 HALF_OPEN 并在飞
    followers = await asyncio.gather(call(), call())
    probe_body = await probe

    print_report("probe-guard: probe in flight", probe_body, await gw.circuits())
    for body in followers:
        r = next(x for x in body["results"] if x["upstream"] == "orders")
        assert r["reason"] == "circuit_rejected"
        assert r["degraded"] is True
        assert r["circuit_state"] == "HALF_OPEN"
        print_report("probe-guard: follower rejected", body)
