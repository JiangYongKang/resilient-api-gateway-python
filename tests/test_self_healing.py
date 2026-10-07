"""自愈探测（后台主动恢复）测试。

覆盖：
- 上游熔断后无需任何客户端请求/人工干预，后台探测自动发现恢复并回到正常放行；
- 并发打满 + 总预算很紧的极端压力下，探测仍然收敛，上游最终恢复；
- 探测在飞期间其它请求得到可区分的降级结果（circuit_rejected）；
- 长期恢复不了的上游：探测有退避节流，不一直烧预算；
- /recovery 只读观测入口反映恢复进展。
"""

from __future__ import annotations

import asyncio
import time

import pytest

from gateway.config import ProbeConfig
from tests.conftest import print_report


def _fast_probe(**overrides) -> ProbeConfig:
    params = {"enabled": True, "interval": 0.1, "timeout": 1.0,
              "acquire_timeout": 5.0, "max_concurrent_probes": 1,
              "backoff_multiplier": 2.0, "max_backoff": 2.0}
    params.update(overrides)
    return ProbeConfig(**params)


async def _wait_recovered(gw, name: str, timeout: float = 10.0) -> dict:
    """轮询只读观测入口，直到指定上游恢复到正常放行。"""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        recovery = await gw.recovery()
        info = recovery["upstreams"][name]
        if info["recovered"]:
            return info
        await asyncio.sleep(0.05)
    raise AssertionError(f"upstream {name!r} did not recover in {timeout}s: "
                         f"{await gw.recovery()}")


@pytest.mark.asyncio
async def test_auto_recovery_without_manual_intervention(gateway_factory,
                                                         mock_cluster):
    # orders 持续 500 -> 熔断 OPEN；之后 mock 恢复正常，
    # 不再发任何聚合请求，后台探测应自动完成恢复
    mock_cluster.set_mode("orders", "error")
    gw = await gateway_factory(failure_threshold=2, recovery_timeout=0.3,
                               probe=_fast_probe())

    for i in range(1, 3):
        resp = await gw.aggregate()
        by = {r["upstream"]: r for r in resp.json()["results"]}
        assert by["orders"]["ok"] is False, f"attempt #{i}"
    states = (await gw.circuits())["states"]
    assert states["orders"] == "OPEN"
    assert states["users"] == "CLOSED"

    # 上游悄悄恢复；此后没有任何客户端聚合请求
    mock_cluster.set_mode("orders", "ok")

    info = await _wait_recovered(gw, "orders")
    print(f"\n===== self-heal: auto recovery =====\n  recovery={info}")
    assert info["attempts"] >= 1
    assert info["successes"] >= 1
    assert info["last_result"] == "success"
    assert (await gw.circuits())["states"]["orders"] == "CLOSED"

    # 恢复后的第一次正常聚合即拿到真实数据
    resp = await gw.aggregate()
    body = resp.json()
    print_report("self-heal: aggregate after auto recovery", body,
                 await gw.circuits())
    by = {r["upstream"]: r for r in body["results"]}
    assert by["orders"]["ok"] is True
    assert by["orders"]["data"] == {"value": "orders"}
    assert by["orders"]["degraded"] is False


@pytest.mark.asyncio
async def test_probe_converges_under_saturated_concurrency_and_tight_budget(
        gateway_factory, mock_cluster):
    # 极端压力：并发上限=2，users/payments 持续慢请求把信号量打满，
    # 总预算卡得很紧；orders 熔断后恢复，探测仍须最终真正打到上游并收敛
    mock_cluster.set_mode("users", "slow", delay=0.4)
    mock_cluster.set_mode("payments", "slow", delay=0.4)
    mock_cluster.set_mode("orders", "error")
    gw = await gateway_factory(
        total_timeout=0.6, upstream_timeout=0.5,
        failure_threshold=1, recovery_timeout=0.2,
        max_concurrency=2,
        probe=_fast_probe(interval=0.1, timeout=0.4, acquire_timeout=8.0))

    # 一次聚合即熔断 orders（阈值=1）
    resp = await gw.aggregate()
    by = {r["upstream"]: r for r in resp.json()["results"]}
    assert by["orders"]["circuit_state"] == "OPEN"

    # orders 悄悄恢复，但客户端流量持续打满并发
    mock_cluster.set_mode("orders", "ok")
    stop_traffic = asyncio.Event()

    async def hammer() -> None:
        while not stop_traffic.is_set():
            await gw.aggregate()

    traffic = [asyncio.ensure_future(hammer()) for _ in range(6)]
    try:
        info = await _wait_recovered(gw, "orders", timeout=15.0)
    finally:
        stop_traffic.set()
        await asyncio.gather(*traffic, return_exceptions=True)

    print(f"\n===== self-heal: converged under saturation =====\n"
          f"  recovery={info}")
    assert info["attempts"] >= 1
    assert info["last_result"] == "success"
    assert (await gw.circuits())["states"]["orders"] == "CLOSED"

    # 恢复后正常聚合能拿到 orders 的真实数据
    resp = await gw.aggregate()
    body = resp.json()
    print_report("self-heal: aggregate after saturated recovery", body,
                 await gw.circuits())
    by = {r["upstream"]: r for r in body["results"]}
    assert by["orders"]["ok"] is True
    assert by["orders"]["data"] == {"value": "orders"}


@pytest.mark.asyncio
async def test_requests_during_probe_get_distinct_degraded_reason(
        gateway_factory, mock_cluster):
    # 探测在飞期间，其它客户端请求必须得到能与"上游真的报错"区分的降级结果
    mock_cluster.set_mode("orders", "error")
    gw = await gateway_factory(failure_threshold=1, recovery_timeout=0.2,
                               probe=_fast_probe())

    await gw.aggregate()
    assert (await gw.circuits())["states"]["orders"] == "OPEN"

    # orders 变成慢但健康：探测会在飞约 0.6s，期间客户端请求应被降级拒绝
    mock_cluster.set_mode("orders", "slow", delay=0.6)

    # 等到探测真的在飞（只读观测入口同步）
    deadline = time.monotonic() + 5.0
    while time.monotonic() < deadline:
        info = (await gw.recovery())["upstreams"]["orders"]
        if info["in_flight"]:
            break
        await asyncio.sleep(0.03)
    else:
        raise AssertionError("probe never went in flight")

    resp = await gw.aggregate()
    body = resp.json()
    print_report("self-heal: request while probe in flight", body,
                 await gw.circuits())
    by = {r["upstream"]: r for r in body["results"]}
    assert by["orders"]["ok"] is False
    assert by["orders"]["reason"] == "circuit_rejected"  # 与真实报错可区分
    assert by["orders"]["degraded"] is True
    assert by["orders"]["data"] is None
    # 健康上游不受探测影响
    assert by["users"]["ok"] is True
    assert by["payments"]["ok"] is True

    # 探测完成后（slow 但成功）上游自动恢复
    info = await _wait_recovered(gw, "orders")
    assert info["last_result"] == "success"


@pytest.mark.asyncio
async def test_persistent_failure_is_throttled_by_backoff(gateway_factory,
                                                          mock_cluster):
    # orders 一直恢复不了：探测持续进行但按指数退避节流，不一直烧预算
    mock_cluster.set_mode("orders", "error")
    gw = await gateway_factory(
        failure_threshold=1, recovery_timeout=0.2,
        probe=_fast_probe(interval=0.1, backoff_multiplier=2.0,
                          max_backoff=1.0))

    await gw.aggregate()
    assert (await gw.circuits())["states"]["orders"] == "OPEN"

    # 等退避到达上限（1.0s）附近：连续失败若干次
    deadline = time.monotonic() + 8.0
    while time.monotonic() < deadline:
        info = (await gw.recovery())["upstreams"]["orders"]
        if info["consecutive_failures"] >= 4:
            break
        await asyncio.sleep(0.1)
    else:
        raise AssertionError(f"probes did not accumulate failures: {info}")

    # 退避到顶后，一个 1.2s 窗口内探测次数有界（<=3），不会高频烧预算
    before = (await gw.recovery())["upstreams"]["orders"]["attempts"]
    await asyncio.sleep(1.2)
    after_info = (await gw.recovery())["upstreams"]["orders"]
    print(f"\n===== self-heal: backoff throttling =====\n"
          f"  attempts {before} -> {after_info['attempts']} in 1.2s, "
          f"info={after_info}")
    assert 0 <= after_info["attempts"] - before <= 3

    # 观测入口完整反映进展：一直在努力、一直失败、尚未恢复
    assert after_info["attempts"] >= 4
    assert after_info["successes"] == 0
    assert after_info["last_result"] == "failure"
    assert after_info["last_error"]
    assert after_info["recovered"] is False
    assert (await gw.circuits())["states"]["orders"] == "OPEN"

    # 观测入口附带探测配置与运行状态
    recovery = await gw.recovery()
    assert recovery["enabled"] is True
    assert recovery["prober_running"] is True
    assert recovery["config"]["max_backoff"] == 1.0
