"""配置项：上游定义、超时预算、熔断与并发参数。

所有时间单位均为秒。可用环境变量覆盖总预算/并发/熔断阈值，便于本地验证。
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field


@dataclass(frozen=True)
class UpstreamConfig:
    name: str
    base_url: str
    path: str = "/data"
    timeout: float = 1.0

    @property
    def url(self) -> str:
        return f"{self.base_url.rstrip('/')}{self.path}"


@dataclass(frozen=True)
class BreakerConfig:
    # 连续失败达到该阈值后熔断打开（单次/偶发失败不会触发）
    failure_threshold: int = 3
    # OPEN 状态冷却秒数；之后放行一次探测进入 HALF_OPEN
    recovery_timeout: float = 5.0


@dataclass(frozen=True)
class GatewayConfig:
    upstreams: tuple[UpstreamConfig, ...] = field(default_factory=tuple)
    # 整个聚合请求的统一时间预算（秒）：任何慢上游都不能把整体拖过该预算
    total_timeout: float = 2.0
    # 单个上游默认超时（秒）；UpstreamConfig.timeout 可逐个覆盖
    default_upstream_timeout: float = 1.0
    # 全局同时进行中的上游 HTTP 调用上限（并发负载控制）
    max_concurrent_calls: int = 8
    breaker: BreakerConfig = field(default_factory=BreakerConfig)


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    return float(raw) if raw else default


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    return int(raw) if raw else default


def build_config(upstreams: tuple[UpstreamConfig, ...],
                 total_timeout: float | None = None,
                 default_upstream_timeout: float | None = None,
                 max_concurrent_calls: int | None = None,
                 failure_threshold: int | None = None,
                 recovery_timeout: float | None = None) -> GatewayConfig:
    """显式构造配置；未提供的项回退到环境变量再回退到默认值。"""
    return GatewayConfig(
        upstreams=upstreams,
        total_timeout=total_timeout if total_timeout is not None
        else _env_float("GW_TOTAL_TIMEOUT", 2.0),
        default_upstream_timeout=default_upstream_timeout
        if default_upstream_timeout is not None
        else _env_float("GW_UPSTREAM_TIMEOUT", 1.0),
        max_concurrent_calls=max_concurrent_calls
        if max_concurrent_calls is not None
        else _env_int("GW_MAX_CONCURRENCY", 8),
        breaker=BreakerConfig(
            failure_threshold=failure_threshold
            if failure_threshold is not None
            else _env_int("GW_BREAKER_THRESHOLD", 3),
            recovery_timeout=recovery_timeout
            if recovery_timeout is not None
            else _env_float("GW_BREAKER_RECOVERY", 5.0),
        ),
    )


def load_config(base_urls: dict[str, str] | None = None) -> GatewayConfig:
    """根据本地 mock 上游地址构造默认配置。

    base_urls: {上游名: 服务根地址}。main.py 启动 mock 后传入。
    """
    base_urls = base_urls or {}
    upstreams = tuple(
        UpstreamConfig(name=name, base_url=url,
                       timeout=_env_float("GW_UPSTREAM_TIMEOUT", 1.0))
        for name, url in base_urls.items()
    )
    return build_config(upstreams)
