"""部分失败语义测试。

覆盖：成功/500/畸形数据混合、全部失败返回 503、404 业务失败不熔断、
失败原因可区分、失败上游 data 恒为 null（不伪造补齐）、结果顺序稳定。
"""

from __future__ import annotations

import pytest

from tests.conftest import DEFAULT_UPSTREAMS, print_report


@pytest.mark.asyncio
async def test_mixed_partial_success_with_distinct_reasons(gateway_factory,
                                                            mock_cluster):
    mock_cluster.set_mode("users", "ok")
    mock_cluster.set_mode("orders", "error")            # HTTP 500
    mock_cluster.set_mode("payments", "malformed")      # 坏 JSON

    gw = await gateway_factory(failure_threshold=10)  # 本用例不关心熔断
    resp = await gw.aggregate()
    assert resp.status_code == 200
    body = resp.json()
    circuits = await gw.circuits()
    print_report("partial: ok + 500 + malformed", body, circuits)

    assert body["success"] is True
    assert body["all_failed"] is False

    by_name = {r["upstream"]: r for r in body["results"]}
    # 结果按配置顺序排列，与完成先后无关
    assert [r["upstream"] for r in body["results"]] == list(DEFAULT_UPSTREAMS)

    users = by_name["users"]
    assert users["ok"] is True
    assert users["data"] == {"value": "users"}  # 真实数据
    assert users["reason"] is None

    orders = by_name["orders"]
    assert orders["ok"] is False
    assert orders["reason"] == "http_error"
    assert orders["data"] is None                # 绝不用伪造数据补齐
    assert "500" in orders["error"]

    payments = by_name["payments"]
    assert payments["ok"] is False
    assert payments["reason"] == "bad_response"
    assert payments["data"] is None

    # 两类失败原因必须可区分
    assert orders["reason"] != payments["reason"]
    # 每个上游都出现且仅出现一次，未被静默丢弃
    assert len(body["results"]) == len(DEFAULT_UPSTREAMS)


@pytest.mark.asyncio
async def test_all_upstreams_fail_returns_503(gateway_factory, mock_cluster):
    mock_cluster.set_mode("users", "error")
    mock_cluster.set_mode("orders", "malformed")
    mock_cluster.set_mode("payments", "not_found")

    gw = await gateway_factory(failure_threshold=10)
    resp = await gw.aggregate()
    assert resp.status_code == 503
    body = resp.json()
    print_report("all failed -> 503", body, await gw.circuits())

    assert body["success"] is False
    assert body["all_failed"] is True
    reasons = {r["upstream"]: r["reason"] for r in body["results"]}
    assert reasons == {
        "users": "http_error",
        "orders": "bad_response",
        "payments": "upstream_not_found",
    }
    assert all(r["data"] is None for r in body["results"])
    assert all(r["error"] for r in body["results"])


@pytest.mark.asyncio
async def test_404_is_business_failure_and_does_not_trip_breaker(
        gateway_factory, mock_cluster):
    mock_cluster.set_mode("users", "ok")
    mock_cluster.set_mode("orders", "not_found")
    mock_cluster.set_mode("payments", "ok")

    gw = await gateway_factory(failure_threshold=2, recovery_timeout=30)
    for attempt in range(5):
        resp = await gw.aggregate()
        body = resp.json()
        assert resp.status_code == 200
        orders = next(r for r in body["results"] if r["upstream"] == "orders")
        assert orders["ok"] is False
        assert orders["reason"] == "upstream_not_found"
        # 404 不应触发熔断，也不应导致降级
        assert orders["degraded"] is False
        assert orders["circuit_state"] == "CLOSED"
    circuits = await gw.circuits()
    print_report("404 repeated must not open breaker (attempt 5)",
                 body, circuits)
    assert circuits["states"]["orders"] == "CLOSED"
    assert circuits["snapshots"]["orders"]["failure_count"] == 0
