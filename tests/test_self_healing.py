"""自愈探测（后台主动恢复）测试。

覆盖：
- 上游熔断后无需任何客户端请求/人工干预，后台探测自动发现恢复并拉回 CLOSED；
- 并发打满 + 预算紧张的极端场景下探测仍然收敛（最终真正打到上游）；
- 长期恢复不了的上游：探测有节流与指数退避，不持续消耗预算，且永不彻底放弃；
- 探测在飞期间，并发请求得到可与真实上游报错区分的降级结果（circuit_rejected）；
- 探测开关关闭时退化为原有的“被动半开”行为。
"""

from __future__ import annotations

import asyncio
import time

import pytest

from gateway.config import ProbeConfig
from tests.conftest import print_report


async def _wait_for(predicate, timeout: float = 8.0, interval: float = 0.05):
    """轮询直到条件满足；返回最后一次观测值（超时则为该值，调用方断言）。"""
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        last = await predicate()
        if last:
            return last
        await asyncio.sleep(interval)
    return last


async def _open_breaker(gw, name: str, times: int) -> None:
    for i in range(1, times + 1):
        body = (await gw.aggregate()).json()
        print_report(f"self-heal: {name} failing #{i}", body)
    assert (await gw.circuits())["states"][name] == "OPEN"


@pytest.mark.asyncio
async def test_auto_recovery_without_any_client_traffic(gateway_factory,
                                                        mock_cluster):
    # orders 连续失败熔断；之后不再发任何聚合请求，
    # 上游恢复后应由后台探测自动发现并拉回 CLOSED。
    mock_cluster.set_mode("orders", "error")
    gw = await gateway_factory(
        failure_threshold=2, recovery_timeout=0.3,
        probe=ProbeConfig(interval=0.1, timeout=1.0, base_backoff=0.2,
                          max_backoff=1.0, max_attempts=3))

    await _open_breaker(gw, "orders", 2)

    # 上游悄悄恢复；全程不再调用 /aggregate
    mock_cluster.set_mode("orders", "ok")

    async def orders_recovered():
        snap = await gw.recovery()
        return snap["upstreams"]["orders"]["recovered"]
    assert await _wait_for(orders_recovered, timeout=8.0), \
        f"not recovered: {await gw.recovery()}"

    snap = await gw.recovery()
    stats = snap["upstreams"]["orders"]
    print(f"\n===== self-heal: auto recovery =====\n  recovery={stats}")
    assert stats["last_result"] == "success"
    assert stats["total_probes"] >= 1
    assert stats["recovered"] is True
    assert (await gw.circuits())["states"]["orders"] == "CLOSED"

    # 恢复后的第一次正常聚合就能拿到真实数据
    body = (await gw.aggregate()).json()
    print_report("self-heal: first aggregate after auto recovery", body)
    orders = next(r for r in body["results"] if r["upstream"] == "orders")
    assert orders["ok"] is True
    assert orders["degraded"] is False
    assert orders["data"] == {"value": "orders"}
    assert body["success"] is True


@pytest.mark.asyncio
async def test_probe_converges_under_saturation_and_tight_budget(
        gateway_factory, mock_cluster):
    # 极端场景：并发上限=1，健康上游都慢（持续占住信号量），预算卡紧。
    # orders 熔断后恢复，探测必须在持续饱和的流量下最终真正打到上游。
    mock_cluster.set_mode("users", "slow", delay=0.25)
    mock_cluster.set_mode("payments", "slow", delay=0.25)
    mock_cluster.set_mode("orders", "error")
    gw = await gateway_factory(
        total_timeout=1.5, upstream_timeout=1.0, max_concurrency=1,
        failure_threshold=2, recovery_timeout=0.3,
        probe=ProbeConfig(interval=0.05, timeout=0.8, base_backoff=0.1,
                          max_backoff=0.5, max_attempts=3))

    await _open_breaker(gw, "orders", 2)

    # 持续制造饱和流量：两个循环不间断地打 /aggregate，把信号量占满
    stop = asyncio.Event()

    async def hammer():
        while not stop.is_set():
            await gw.aggregate()

    hammers = [asyncio.ensure_future(hammer()) for _ in range(2)]
    try:
        await asyncio.sleep(0.2)  # 确认探测在饱和下也抢不到槽位时不会卡死
        mock_cluster.set_mode("orders", "ok")  # 上游恢复

        async def orders_recovered():
            snap = await gw.recovery()
            return snap["upstreams"]["orders"]["recovered"]

        ok = await _wait_for(orders_recovered, timeout=10.0)
        snap = await gw.recovery()
        print(f"\n===== self-heal: converge under saturation =====\n"
              f"  recovery={snap['upstreams']['orders']}")
        assert ok, f"probe never converged: {snap['upstreams']['orders']}"
    finally:
        stop.set()
        await asyncio.gather(*hammers, return_exceptions=True)

    stats = (await gw.recovery())["upstreams"]["orders"]
    assert stats["last_result"] == "success"
    assert stats["total_probes"] >= 1

    # 恢复后正常聚合拿到真实数据；健康上游没有被探测挤垮
    body = (await gw.aggregate()).json()
    print_report("self-heal: aggregate after saturated recovery", body)
    by = {r["upstream"]: r for r in body["results"]}
    assert by["orders"]["ok"] is True
    assert by["orders"]["data"] == {"value": "orders"}
    assert by["users"]["ok"] is True
    assert by["payments"]["ok"] is True


@pytest.mark.asyncio
async def test_persistent_failure_is_throttled_with_backoff_but_never_gives_up(
        gateway_factory, mock_cluster):
    # orders 一直坏：探测次数必须被退避节流（远少于无退避时的理论值），
    # 每次失败可见、退避窗口增长有上限；且探测永不彻底停止——
    # 上游一旦恢复，仍能自动收敛。
    mock_cluster.set_mode("orders", "error")
    gw = await gateway_factory(
        failure_threshold=2, recovery_timeout=0.1,
        probe=ProbeConfig(interval=0.05, timeout=0.5, base_backoff=0.2,
                          max_backoff=0.8, max_attempts=3))

    await _open_breaker(gw, "orders", 2)

    await asyncio.sleep(2.5)  # 静默观察期：没有任何客户端流量
    snap = await gw.recovery()
    stats = snap["upstreams"]["orders"]
    print(f"\n===== self-heal: backoff throttling =====\n  recovery={stats}")

    assert stats["recovered"] is False
    assert stats["last_result"] == "failure"
    assert stats["attempts"] == stats["total_probes"]  # 每次都真实失败
    # 退避 0.2/0.4/0.8(饱和) + 冷却 0.1：2.5s 内大约 4~6 次；
    # 若无退避（仅 0.05s 扫描间隔）理论值约 15+ 次
    assert 2 <= stats["total_probes"] <= 9, stats
    # 探测期间熔断器保持 OPEN，客户端请求始终快速失败（可区分降级）
    body = (await gw.aggregate()).json()
    orders = next(r for r in body["results"] if r["upstream"] == "orders")
    assert orders["reason"] == "circuit_open"
    assert orders["degraded"] is True

    # 永不放弃：上游恢复后下一轮探测即收敛
    mock_cluster.set_mode("orders", "ok")

    async def orders_recovered():
        return (await gw.recovery())["upstreams"]["orders"]["recovered"]

    assert await _wait_for(orders_recovered, timeout=8.0)
    final = (await gw.recovery())["upstreams"]["orders"]
    assert final["last_result"] == "success"
    assert (await gw.circuits())["states"]["orders"] == "CLOSED"


@pytest.mark.asyncio
async def test_requests_during_probe_get_distinguishable_rejection(
        gateway_factory, mock_cluster):
    # 探测在飞期间，并发客户端请求得到 circuit_rejected（降级、可区分），
    # 而不是被误认为上游真实报错；探测完成后自动恢复。
    mock_cluster.set_mode("orders", "error")
    gw = await gateway_factory(
        failure_threshold=1, recovery_timeout=0.2,
        upstream_timeout=2.0, total_timeout=3.0,
        probe=ProbeConfig(interval=0.05, timeout=2.0, base_backoff=0.2,
                          max_backoff=1.0, max_attempts=3))

    await _open_breaker(gw, "orders", 1)

    # 上游变成慢响应：探测会真实打到上游并在飞约 1s
    mock_cluster.set_mode("orders", "slow", delay=1.0)

    async def probe_in_flight():
        circuits = await gw.circuits()
        return circuits["snapshots"]["orders"]["probe_in_flight"]

    assert await _wait_for(probe_in_flight, timeout=5.0), \
        "background probe never started"

    # 探测在飞时来一个客户端请求：必须被可区分地拒绝
    body = (await gw.aggregate()).json()
    print_report("self-heal: request during in-flight probe", body)
    orders = next(r for r in body["results"] if r["upstream"] == "orders")
    assert orders["ok"] is False
    assert orders["reason"] == "circuit_rejected"
    assert orders["degraded"] is True
    assert orders["circuit_state"] == "HALF_OPEN"

    # 探测成功（slow 是 200），上游自动恢复
    async def orders_recovered():
        return (await gw.recovery())["upstreams"]["orders"]["recovered"]

    assert await _wait_for(orders_recovered, timeout=8.0)
    stats = (await gw.recovery())["upstreams"]["orders"]
    assert stats["last_result"] == "success"


@pytest.mark.asyncio
async def test_prober_disabled_falls_back_to_passive_half_open(
        gateway_factory, mock_cluster):
    # 关闭自愈探测：冷却到期后不会自动恢复，直到下一次客户端请求被动触发半开。
    mock_cluster.set_mode("orders", "error")
    gw = await gateway_factory(
        failure_threshold=2, recovery_timeout=0.2,
        probe=ProbeConfig(enabled=False))

    await _open_breaker(gw, "orders", 2)
    mock_cluster.set_mode("orders", "ok")

    await asyncio.sleep(0.8)  # 远超冷却时间，但没有探测器 -> 仍然 OPEN
    assert (await gw.circuits())["states"]["orders"] == "OPEN"
    snap = await gw.recovery()
    assert snap["enabled"] is False
    assert snap["upstreams"]["orders"]["total_probes"] == 0

    # 下一次客户端请求作为被动探测，成功恢复
    body = (await gw.aggregate()).json()
    print_report("self-heal: passive half-open after prober disabled", body)
    orders = next(r for r in body["results"] if r["upstream"] == "orders")
    assert orders["ok"] is True
    assert orders["data"] == {"value": "orders"}
    assert (await gw.circuits())["states"]["orders"] == "CLOSED"
