"""本地 mock 上游：可注入 成功 / 慢响应 / 500 / 畸形数据 / 404 等行为。

仅监听 127.0.0.1，不依赖任何外部网络。行为可在运行时通过 /control 切换，
并记录自身被并发访问的峰值，供并发测试断言。
"""

from __future__ import annotations

import asyncio
import itertools
import threading
import time

from fastapi import FastAPI
from fastapi.responses import JSONResponse, PlainTextResponse, Response
from pydantic import BaseModel

# /data 可切换的行为模式
MODE_OK = "ok"
MODE_SLOW = "slow"
MODE_ERROR = "error"          # HTTP 500
MODE_MALFORMED = "malformed"  # 200 但 body 是坏 JSON
MODE_NOT_FOUND = "not_found"  # HTTP 404


class ControlBody(BaseModel):
    mode: str | None = None
    delay: float | None = None
    payload: dict | None = None
    # 连续 N 次调用按 sequence 逐个返回，之后停留在最后一个模式（可选）
    sequence: list[str] | None = None


def create_mock_app(name: str, mode: str = MODE_OK, delay: float = 0.0,
                    payload: dict | None = None) -> FastAPI:
    app = FastAPI(title=f"mock-upstream-{name}")

    state = {
        "mode": mode,
        "delay": delay,
        "payload": payload if payload is not None else {"value": name},
        "sequence": None,
        "seq_index": 0,
    }
    lock = threading.Lock()
    in_flight = itertools.count(1)
    peak = {"current": 0, "max": 0}

    def _next_mode() -> str:
        """若配置了 sequence 则按调用次序取模式，否则用固定 mode。"""
        seq = state["sequence"]
        if seq:
            idx = min(state["seq_index"], len(seq) - 1)
            state["seq_index"] += 1
            return seq[idx]
        return state["mode"]

    @app.get("/health")
    def health() -> dict:
        return {"upstream": name, "healthy": True}

    @app.get("/data")
    async def data() -> Response:
        token = next(in_flight)
        with lock:
            peak["current"] += 1
            peak["max"] = max(peak["max"], peak["current"])
        try:
            mode = _next_mode()
            delay = state["delay"] if mode == MODE_SLOW else 0.0
            if delay:
                await asyncio.sleep(delay)
            if mode == MODE_OK:
                return JSONResponse({
                    "upstream": name,
                    "request_id": token,
                    "data": state["payload"],
                })
            if mode == MODE_SLOW:
                return JSONResponse({
                    "upstream": name,
                    "request_id": token,
                    "data": {**state["payload"], "slept": delay},
                })
            if mode == MODE_ERROR:
                return JSONResponse(status_code=500,
                                    content={"error": "mock upstream failure",
                                             "upstream": name})
            if mode == MODE_NOT_FOUND:
                return JSONResponse(status_code=404,
                                    content={"error": "not found", "upstream": name})
            if mode == MODE_MALFORMED:
                # 200 + 声称 JSON，但 body 无法解析
                return Response(content='{"broken": ',
                                media_type="application/json")
            return JSONResponse(status_code=500,
                                content={"error": f"unknown mode {mode}"})
        finally:
            with lock:
                peak["current"] -= 1

    @app.post("/control")
    def control(body: ControlBody) -> dict:
        if body.mode is not None:
            state["mode"] = body.mode
        if body.delay is not None:
            state["delay"] = body.delay
        if body.payload is not None:
            state["payload"] = body.payload
        if body.sequence is not None:
            state["sequence"] = body.sequence
            state["seq_index"] = 0
        return {"upstream": name, **state_summary()}

    @app.get("/control")
    def get_control() -> dict:
        return {"upstream": name, **state_summary()}

    def state_summary() -> dict:
        with lock:
            return {
                "mode": state["mode"],
                "delay": state["delay"],
                "sequence": state["sequence"],
                "seq_index": state["seq_index"],
                "in_flight_peak": peak["max"],
            }

    @app.get("/__clock")
    def clock() -> dict:
        """供测试/探活用：返回单调时钟，无副作用。"""
        return {"t": time.monotonic()}

    return app
