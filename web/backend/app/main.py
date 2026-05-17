from __future__ import annotations

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .database import init_database
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


@app.get("/api/health")
def health_check() -> dict:
    return {"status": "ok"}


@app.get("/api/dashboard")
def get_dashboard() -> dict:
    return get_dashboard_data()
