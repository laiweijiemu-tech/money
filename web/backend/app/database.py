from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from .services.mock_data import seed_dashboard_data

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
DB_PATH = DATA_DIR / "watchtower.db"


def get_connection() -> sqlite3.Connection:
    connection = sqlite3.connect(DB_PATH)
    connection.row_factory = sqlite3.Row
    return connection


def init_database() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    payload = seed_dashboard_data()
    with get_connection() as connection:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS dashboard_snapshots (
                snapshot_key TEXT PRIMARY KEY,
                payload_json TEXT NOT NULL
            )
            """
        )
        connection.execute(
            """
            INSERT OR REPLACE INTO dashboard_snapshots (snapshot_key, payload_json)
            VALUES (?, ?)
            """,
            ("default", json.dumps(payload, ensure_ascii=False)),
        )
        connection.commit()


def save_dashboard_snapshot(payload: dict) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with get_connection() as connection:
        connection.execute(
            """
            INSERT OR REPLACE INTO dashboard_snapshots (snapshot_key, payload_json)
            VALUES (?, ?)
            """,
            ("default", json.dumps(payload, ensure_ascii=False)),
        )
        connection.commit()


def load_dashboard_snapshot() -> dict:
    with get_connection() as connection:
        row = connection.execute(
            """
            SELECT payload_json
            FROM dashboard_snapshots
            WHERE snapshot_key = ?
            """,
            ("default",),
        ).fetchone()
    if row is None:
        init_database()
        return load_dashboard_snapshot()
    return json.loads(row["payload_json"])
