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
class ProbeConfig:
    """自愈探测（后台主动恢复）参数。

    探测复用与正常调用相同的总预算/信号量约束，同一上游同一时刻最多一次探测。
    """
    # 是否启用后台自愈探测（关闭后退回“等下一次客户端请求被动触发”的行为）
    enabled: bool = True
    # 后台扫描间隔（秒）：每隔多久检查一次是否有冷却到期的熔断器
    interval: float = 0.5
    # 单次探测的统一时间预算（秒）：等信号量 + 真正请求都在该预算内
    timeout: float = 2.0
    # 探测失败后的基础退避（秒），按连续失败次数指数增长
    base_backoff: float = 0.5
    # 退避上限（秒）：连续失败次数超过 max_attempts 后固定按该间隔探测，
    # 永不彻底停止（保证上游恢复后总能收敛），但不会把预算持续耗在坏上游上
    max_backoff: float = 10.0
    # 退避增长所计的最大连续失败次数（达到后间隔饱和为 max_backoff）
    max_attempts: int = 5


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
    probe: ProbeConfig = field(default_factory=ProbeConfig)


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    return float(raw) if raw else default


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    return int(raw) if raw else default


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if not raw:
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


def build_config(upstreams: tuple[UpstreamConfig, ...],
                 total_timeout: float | None = None,
                 default_upstream_timeout: float | None = None,
                 max_concurrent_calls: int | None = None,
                 failure_threshold: int | None = None,
                 recovery_timeout: float | None = None,
                 probe: ProbeConfig | None = None) -> GatewayConfig:
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
        probe=probe if probe is not None else ProbeConfig(
            enabled=_env_bool("GW_PROBE_ENABLED", True),
            interval=_env_float("GW_PROBE_INTERVAL", 0.5),
            timeout=_env_float("GW_PROBE_TIMEOUT", 2.0),
            base_backoff=_env_float("GW_PROBE_BACKOFF", 0.5),
            max_backoff=_env_float("GW_PROBE_MAX_BACKOFF", 10.0),
            max_attempts=_env_int("GW_PROBE_MAX_ATTEMPTS", 5),
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
