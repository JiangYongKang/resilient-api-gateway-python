"""Local mock upstream services (no external network).

Each mock upstream exposes:
- GET  /upstream/{name}/data  -> the data endpoint the gateway aggregates
- POST /upstream/{name}/mode  -> switch behaviour: ok | slow | error | malformed
- GET  /upstream/{name}/stats -> how many data requests were received
"""
from __future__ import annotations

import asyncio

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

VALID_MODES = ("ok", "slow", "error", "malformed")


class ModeRequest(BaseModel):
    mode: str
    delay_s: float = 0.0


def create_mock_upstream_app(names: list[str]) -> FastAPI:
    """Build a mock upstream app with controllable behaviours."""
    app = FastAPI(title="mock-upstreams")
    app.state.modes = {n: ModeRequest(mode="ok") for n in names}
    app.state.counts = {n: 0 for n in names}
    app.state.inflight = {n: 0 for n in names}
    app.state.max_inflight = {n: 0 for n in names}

    @app.get("/upstream/{name}/data")
    async def data(name: str) -> dict:
        if name not in app.state.modes:
            raise HTTPException(status_code=404, detail="unknown upstream")
        app.state.counts[name] += 1
        app.state.inflight[name] += 1
        app.state.max_inflight[name] = max(
            app.state.max_inflight[name], app.state.inflight[name]
        )
        try:
            cfg = app.state.modes[name]
            if cfg.mode == "slow":
                await asyncio.sleep(cfg.delay_s)
                return {"payload": {"upstream": name, "value": f"late-{name}"}}
            if cfg.mode == "error":
                raise HTTPException(status_code=503, detail=f"{name} is broken")
            if cfg.mode == "malformed":
                return {"unexpected": "no payload field here"}
            if cfg.delay_s > 0:
                await asyncio.sleep(cfg.delay_s)
            return {"payload": {"upstream": name, "value": f"hello-from-{name}"}}
        finally:
            app.state.inflight[name] -= 1

    @app.post("/upstream/{name}/mode")
    async def set_mode(name: str, req: ModeRequest) -> dict:
        if name not in app.state.modes:
            raise HTTPException(status_code=404, detail="unknown upstream")
        if req.mode not in VALID_MODES:
            raise HTTPException(status_code=422, detail=f"mode must be one of {VALID_MODES}")
        app.state.modes[name] = req
        return {"name": name, "mode": req.mode, "delay_s": req.delay_s}

    @app.get("/upstream/{name}/stats")
    async def stats(name: str) -> dict:
        if name not in app.state.counts:
            raise HTTPException(status_code=404, detail="unknown upstream")
        return {
            "name": name,
            "requests_received": app.state.counts[name],
            "max_concurrent_observed": app.state.max_inflight[name],
        }

    return app



def _serve(name: str, port: int) -> None:
    """Module-level so multiprocessing spawn can import it."""
    import uvicorn

    uvicorn.run(create_mock_upstream_app([name]),
                host="127.0.0.1", port=port, log_level="warning")


if __name__ == "__main__":
    # Run all mock upstreams in one process, one port per upstream.
    import multiprocessing

    procs = [
        multiprocessing.Process(target=_serve, args=(n, p))
        for n, p in [("alpha", 9001), ("beta", 9002), ("gamma", 9003)]
    ]
    for p in procs:
        p.start()
    for p in procs:
        p.join()
