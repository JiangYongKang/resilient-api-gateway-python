"""请求/响应数据模型与失败原因枚举。

设计原则：
- 每个上游都必须在 results 中出现一条结果，成功/失败显式区分，绝不静默丢弃；
- 失败结果携带可区分的 reason（失败原因分类）与 error（人类可读说明）；
- 不使用任何伪造数据补齐：失败上游的 data 恒为 None。
"""

from __future__ import annotations

from enum import Enum

from pydantic import BaseModel, Field


class FailReason(str, Enum):
    """上游调用失败的可区分原因。"""

    TIMEOUT = "timeout"                      # 单上游超过其超时
    BUDGET_EXCEEDED = "budget_exceeded"      # 总预算已耗尽，调用被取消或未及发起
    HTTP_ERROR = "http_error"                # 上游返回 5xx
    UPSTREAM_ERROR = "upstream_error"        # 连接失败 / 网络错误
    BAD_RESPONSE = "bad_response"            # 非 JSON / 数据结构畸形
    UPSTREAM_NOT_FOUND = "upstream_not_found"  # 上游返回 404（业务性失败，不熔断）
    CIRCUIT_OPEN = "circuit_open"            # 熔断打开，快速失败（降级）
    CIRCUIT_REJECTED = "circuit_rejected"    # 半开探测进行中，其他请求被拒绝（降级）
    INTERNAL_ERROR = "internal_error"        # 网关内部异常


# 被熔断器降级（未真正打到上游）的两类原因
DEGRADED_REASONS = frozenset({FailReason.CIRCUIT_OPEN, FailReason.CIRCUIT_REJECTED})


class CircuitState(str, Enum):
    CLOSED = "CLOSED"   # 正常放行
    OPEN = "OPEN"       # 快速失败
    HALF_OPEN = "HALF_OPEN"  # 冷却后放行一次探测


class UpstreamResult(BaseModel):
    """单个上游的结果。成功时 data 有值；失败时 reason/error 有值。"""

    upstream: str
    ok: bool
    data: dict | None = None
    error: str | None = None
    reason: FailReason | None = None
    degraded: bool = False  # True 表示因熔断降级而未真正请求上游
    elapsed_ms: int = 0
    circuit_state: CircuitState = CircuitState.CLOSED


class AggregateResponse(BaseModel):
    """聚合响应。results 按上游配置顺序排列，保证并发下输出稳定。"""

    success: bool                    # 至少一个上游成功
    all_failed: bool                 # 所有上游都失败
    total_ms: int = 0
    timed_out: bool = False          # 是否触达总时间预算
    results: list[UpstreamResult] = Field(default_factory=list)

    @property
    def failed(self) -> list[UpstreamResult]:
        return [r for r in self.results if not r.ok]

    @property
    def succeeded(self) -> list[UpstreamResult]:
        return [r for r in self.results if r.ok]
