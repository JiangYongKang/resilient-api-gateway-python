"""超时预算测试。

覆盖：
- 单个慢上游超过自身 timeout -> reason=timeout，整体仍返回其他上游；
- 所有上游都慢但慢得“恰好”：整体不会超过统一总预算（硬上限断言）；
- 预算耗尽的失败原因标记为 budget_exceeded，与 timeout 可区分；
- 预算内结果确定可重复（同样的慢度两次运行结论一致）。
"""

from __future__ import annotations

import time

import pytest

from tests.conftest import print_report


@pytest.mark.asyncio
async def test_slow_upstream_hits_per_upstream_timeout(gateway_factory,
                                                        mock_cluster):
    mock_cluster.set_mode("users", "ok")
    mock_cluster.set_mode("orders", "slow", delay=1.0)   # 超过自身 timeout
    mock_cluster.set_mode("payments", "ok")

    gw = await gateway_factory(total_timeout=3.0, upstream_timeout=0.3)
    resp = await gw.aggregate()
    body = resp.json()
    print_report("per-upstream timeout", body, await gw.circuits())

    by = {r["upstream"]: r for r in body["results"]}
    assert resp.status_code == 200
    assert by["users"]["ok"] is True
    assert by["payments"]["ok"] is True
    assert by["orders"]["ok"] is False
    assert by["orders"]["reason"] == "timeout"
    assert by["orders"]["data"] is None
    # 整体耗时约等于自身超时，而不是上游真实慢 1s
    assert body["total_ms"] < 600


@pytest.mark.asyncio
async def test_total_budget_is_hard_upper_bound(gateway_factory, mock_cluster):
    # 三个上游都慢于总预算；单上游 timeout 设得很宽，压力落在总预算上
    for name in ("users", "orders", "payments"):
        mock_cluster.set_mode(name, "slow", delay=2.0)

    total_budget = 0.8
    gw = await gateway_factory(total_timeout=total_budget,
                               upstream_timeout=5.0,
                               max_concurrency=3)
    start = time.monotonic()
    resp = await gw.aggregate()
    elapsed = time.monotonic() - start
    body = resp.json()
    print_report(f"total budget hard bound, real elapsed={elapsed:.3f}s",
                 body, await gw.circuits())

    assert resp.status_code == 503
    assert body["all_failed"] is True
    # 硬上限：任何慢上游组合都不能把整体拖到预算外（留实现余量断言）
    assert elapsed < total_budget + 0.6, elapsed
    assert body["total_ms"] < (total_budget + 0.6) * 1000
    reasons = {r["reason"] for r in body["results"]}
    assert reasons <= {"timeout", "budget_exceeded"}
    assert "budget_exceeded" in reasons
    assert all(r["error"] for r in body["results"])


@pytest.mark.asyncio
async def test_slow_and_fast_mix_respects_budget_and_is_repeatable(
        gateway_factory, mock_cluster):
    mock_cluster.set_mode("users", "ok")
    mock_cluster.set_mode("orders", "slow", delay=2.0)
    mock_cluster.set_mode("payments", "ok")

    # 总预算小于 orders 的自身超时窗口 -> orders 应被判为 budget_exceeded
    gw = await gateway_factory(total_timeout=0.5, upstream_timeout=2.0)

    outcomes = []
    for run in range(2):
        start = time.monotonic()
        body = (await gw.aggregate()).json()
        elapsed = time.monotonic() - start
        print_report(f"budget mix run {run + 1}, elapsed={elapsed:.3f}s",
                     body, await gw.circuits())
        by = {r["upstream"]: r for r in body["results"]}
        outcomes.append({
            "users_ok": by["users"]["ok"],
            "payments_ok": by["payments"]["ok"],
            "orders_reason": by["orders"]["reason"],
            "elapsed": elapsed,
        })
        assert elapsed < 0.5 + 0.5

    # 两次运行结论一致：确定性、可重复
    assert outcomes[0]["users_ok"] == outcomes[1]["users_ok"] is True
    assert outcomes[0]["payments_ok"] == outcomes[1]["payments_ok"] is True
    assert outcomes[0]["orders_reason"] == outcomes[1]["orders_reason"]
    assert outcomes[0]["orders_reason"] == "budget_exceeded"
