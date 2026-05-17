from __future__ import annotations

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .database import init_database
from .services.intraday_monitor import monitor
from .services.live_data import get_dashboard_data

app = FastAPI(
    title="Shortline Watchtower API",
    version="0.1.0",
    summary="面向短线打板场景的轻量级盯盘后端",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
def on_startup() -> None:
    init_database()
    monitor.start()


@app.on_event("shutdown")
def on_shutdown() -> None:
    monitor.stop()


@app.get("/api/health")
def health_check() -> dict:
    return {"status": "ok"}


@app.get("/api/dashboard")
def get_dashboard() -> dict:
    data = get_dashboard_data()
    data["events"] = monitor.get_events()
    return data


@app.get("/api/events")
def get_events() -> dict:
    return {
        "events": monitor.get_events(),
        "live_pool_count": len(monitor.get_live_pool()),
    }
