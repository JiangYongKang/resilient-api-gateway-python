"""上游 HTTP 调用封装（httpx，仅访问 127.0.0.1 mock）。

把底层异常与状态码统一映射成 UpstreamCallError(reason=FailReason...)，
供聚合器记录；asyncio.CancelledError 不属于 Exception，会自然向上传播，
不会被误记成普通上游错误。
"""

from __future__ import annotations

import httpx

from gateway.config import UpstreamConfig
from gateway.models import FailReason


class UpstreamCallError(Exception):
    def __init__(self, reason: FailReason, message: str,
                 status_code: int | None = None) -> None:
        super().__init__(message)
        self.reason = reason
        self.status_code = status_code


class UpstreamClient:
    def __init__(self, config: UpstreamConfig) -> None:
        self.config = config

    async def fetch(self, client: httpx.AsyncClient,
                    deadline_remaining: float) -> dict:
        """发起一次调用。

        deadline_remaining: 总预算剩余秒数；实际等待取 min(自身超时, 预算剩余)，
        保证慢上游不会拖过总预算。
        """
        wait = max(0.0, min(self.config.timeout, deadline_remaining))
        url = self.config.url
        try:
            resp = await client.get(url, timeout=wait)
        except httpx.TimeoutException as exc:
            raise UpstreamCallError(
                FailReason.TIMEOUT,
                f"upstream {self.config.name!r} timed out after {wait:.3f}s: {exc}",
            ) from exc
        except httpx.RequestError as exc:
            raise UpstreamCallError(
                FailReason.UPSTREAM_ERROR,
                f"upstream {self.config.name!r} connection/request failed: {exc}",
            ) from exc

        if resp.status_code >= 500:
            raise UpstreamCallError(
                FailReason.HTTP_ERROR,
                f"upstream {self.config.name!r} returned HTTP {resp.status_code}",
                status_code=resp.status_code,
            )
        if resp.status_code == 404:
            raise UpstreamCallError(
                FailReason.UPSTREAM_NOT_FOUND,
                f"upstream {self.config.name!r} returned HTTP 404",
                status_code=404,
            )
        if resp.status_code >= 400:
            raise UpstreamCallError(
                FailReason.UPSTREAM_ERROR,
                f"upstream {self.config.name!r} returned HTTP {resp.status_code}",
                status_code=resp.status_code,
            )

        try:
            payload = resp.json()
        except ValueError as exc:
            raise UpstreamCallError(
                FailReason.BAD_RESPONSE,
                f"upstream {self.config.name!r} returned invalid JSON: {exc}",
                status_code=resp.status_code,
            ) from exc

        # 畸形数据：顶层不是对象，或缺失 data 字段/类型不对，都视为坏响应
        if not isinstance(payload, dict) or not isinstance(payload.get("data"), dict):
            raise UpstreamCallError(
                FailReason.BAD_RESPONSE,
                f"upstream {self.config.name!r} returned malformed payload: "
                f"missing object 'data'",
                status_code=resp.status_code,
            )
        return payload
