from __future__ import annotations

import logging
import threading
import time
from datetime import datetime, time as dt_time, timedelta
from typing import Any

from .live_data import _build_watchlist, _fetch_sina_quotes, _get_limit_up_threshold

logger = logging.getLogger(__name__)

TICK_INTERVAL = 10
MAX_EVENTS = 50
MAX_LIVE_POOL = 20
SECTOR_ALERT_COOLDOWN = timedelta(minutes=5)
WEAK_TO_STRONG_OPEN_LIMIT = 2.0
WEAK_TO_STRONG_DEADLINE = dt_time(hour=9, minute=40)


def _is_trading_time() -> bool:
    now = datetime.now()
    t = now.time()
    if t.hour == 9 and t.minute >= 25:
        return True
    if t.hour == 10:
        return True
    if t.hour == 11 and t.minute <= 30:
        return True
    if 13 <= t.hour < 15:
        return True
    return False


def _is_weak_to_strong_quick_board(
    open_change_pct: float | None, change_pct: float, code: str, now: datetime
) -> bool:
    if open_change_pct is None:
        return False
    if open_change_pct >= WEAK_TO_STRONG_OPEN_LIMIT:
        return False
    if now.time() > WEAK_TO_STRONG_DEADLINE:
        return False
    return change_pct >= _get_limit_up_threshold(code)


class StockState:
    __slots__ = (
        "symbol", "name", "code", "theme", "change_pct", "open_change_pct",
        "status", "prev_status", "first_limit_time", "break_count",
        "amount", "seal_amount", "last_price", "tag",
    )

    def __init__(self, symbol: str, name: str, code: str, theme: str) -> None:
        self.symbol = symbol
        self.name = name
        self.code = code
        self.theme = theme
        self.change_pct: float = 0.0
        self.open_change_pct: float | None = None
        self.status: str = "normal"
        self.prev_status: str = "normal"
        self.first_limit_time: str | None = None
        self.break_count: int = 0
        self.amount: float = 0.0
        self.seal_amount: float = 0.0
        self.last_price: float = 0.0
        self.tag: str = ""


class IntradayMonitor:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._states: dict[str, StockState] = {}
        self._events: list[dict[str, Any]] = []
        self._live_pool: dict[str, dict[str, Any]] = {}
        self._last_sector_alert_at: dict[str, datetime] = {}
        self._thread: threading.Thread | None = None
        self._running = False

    def start(self) -> None:
        self._running = True
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()
        logger.info("IntradayMonitor started")

    def stop(self) -> None:
        self._running = False
        if self._thread:
            self._thread.join(timeout=15)
        logger.info("IntradayMonitor stopped")

    def get_events(self, limit: int = 20) -> list[dict[str, Any]]:
        with self._lock:
            return list(self._events[-limit:])

    def get_live_pool(self) -> list[dict[str, Any]]:
        with self._lock:
            return list(self._live_pool.values())

    def _loop(self) -> None:
        while self._running:
            try:
                if _is_trading_time():
                    self._tick()
                else:
                    # Outside trading hours, clear state at end of day
                    now = datetime.now().time()
                    if now.hour >= 15 and now.minute >= 5:
                        with self._lock:
                            self._states.clear()
                            self._live_pool.clear()
                            self._last_sector_alert_at.clear()
            except Exception as exc:
                logger.warning(f"Monitor tick error: {exc}")
            time.sleep(TICK_INTERVAL)

    def _tick(self) -> None:
        watchlist = _build_watchlist()
        quotes = _fetch_sina_quotes(watchlist)
        if not quotes:
            return

        watchlist_map = {item["symbol"]: item for item in watchlist}
        now = datetime.now()
        now_str = datetime.now().strftime("%H:%M:%S")
        new_events: list[dict[str, Any]] = []

        with self._lock:
            for item in quotes:
                symbol = item["symbol"]
                code = item["code"]
                name = item["name"]
                pct = item["change_pct"]
                theme = item.get("theme", "")
                config = watchlist_map.get(symbol, {})

                state = self._states.get(symbol)
                if not state:
                    state = StockState(symbol, name, code, theme)
                    self._states[symbol] = state

                state.prev_status = state.status
                state.change_pct = pct
                state.amount = item.get("amount", 0)
                state.seal_amount = item.get("seal_amount", 0)
                state.last_price = item.get("last_price", 0)
                state.tag = item.get("tag", config.get("tag", ""))
                state.theme = theme

                if state.open_change_pct is None:
                    state.open_change_pct = pct

                threshold = _get_limit_up_threshold(code)
                if pct >= threshold:
                    new_status = "limit_up"
                elif pct >= 6:
                    new_status = "surging"
                elif state.prev_status == "limit_up" and pct < threshold:
                    new_status = "broken"
                elif state.prev_status == "broken" and pct < threshold:
                    new_status = "broken"
                else:
                    new_status = "normal"

                state.status = new_status

                # Detect transitions
                if state.prev_status != "limit_up" and new_status == "limit_up":
                    if state.prev_status == "broken":
                        # 炸板回封
                        event = {
                            "type": "limit_up_reseal",
                            "symbol": symbol,
                            "name": name,
                            "code": code,
                            "theme": theme,
                            "message": (
                                f"炸板后再次回封，当前涨幅 {pct:.1f}%，"
                                f"封单约 {state.seal_amount:.2f} 亿。"
                            ),
                            "triggered_at": now_str,
                            "level": "high",
                        }
                        new_events.append(event)
                    else:
                        # 新封板
                        state.first_limit_time = now_str
                        is_weak_to_strong = _is_weak_to_strong_quick_board(
                            state.open_change_pct,
                            pct,
                            code,
                            now,
                        )
                        if is_weak_to_strong:
                            event = {
                                "type": "weak_to_strong",
                                "symbol": symbol,
                                "name": name,
                                "code": code,
                                "theme": theme,
                                "message": (
                                    f"弱转强秒板！开盘涨幅仅 {state.open_change_pct:.1f}%，"
                                    f"当前封单约 {state.seal_amount:.2f} 亿。"
                                ),
                                "triggered_at": now_str,
                                "level": "high",
                            }
                        else:
                            event = {
                                "type": "limit_up_new",
                                "symbol": symbol,
                                "name": name,
                                "code": code,
                                "theme": theme,
                                "message": (
                                    f"盘中封板，涨幅 {pct:.1f}%，"
                                    f"成交 {state.amount / 1e8:.1f} 亿，封单 {state.seal_amount:.2f} 亿。"
                                ),
                                "triggered_at": now_str,
                                "level": "high",
                            }
                        new_events.append(event)

                    # Add to live pool
                    if symbol not in self._live_pool and len(self._live_pool) < MAX_LIVE_POOL:
                        self._live_pool[symbol] = {
                            "symbol": symbol,
                            "theme": theme or "盘中捕获",
                            "tag": config.get("tag", "盘中捕获"),
                            "float_shares": config.get("float_shares", 0),
                            "mktcap": config.get("mktcap", 0),
                            "cost_price": config.get("cost_price"),
                        }

                elif state.prev_status == "limit_up" and new_status == "broken":
                    state.break_count += 1

            # Sector surge detection
            sector_events = self._detect_sector_surge(quotes, now, now_str)
            new_events.extend(sector_events)

            # Append events
            self._events.extend(new_events)
            if len(self._events) > MAX_EVENTS:
                self._events = self._events[-MAX_EVENTS:]

    def _detect_sector_surge(
        self, quotes: list[dict[str, Any]], now: datetime, now_str: str
    ) -> list[dict[str, Any]]:
        events: list[dict[str, Any]] = []

        groups: dict[str, list[dict[str, Any]]] = {}
        for item in quotes:
            theme = item.get("theme", "")
            if not theme:
                continue
            groups.setdefault(theme, []).append(item)

        for theme, members in groups.items():
            if len(members) < 3:
                continue
            if self._is_sector_alert_in_cooldown(theme, now):
                continue

            ranked = sorted(members, key=lambda item: item["change_pct"], reverse=True)
            leader = ranked[0]
            strong_count = sum(1 for item in ranked if item["change_pct"] >= 5)
            limit_count = sum(
                1
                for item in ranked
                if item["change_pct"] >= _get_limit_up_threshold(item["code"])
            )
            followers = [item for item in ranked[1:] if item["change_pct"] >= 2.5]
            if leader["change_pct"] < 5 or len(followers) < 2:
                continue
            if strong_count < 3 and limit_count < 1:
                continue

            follower_names = "、".join(item["name"] for item in followers[:3])
            message = (
                f"{leader['name']}领涨 {theme}，{follower_names}同步走强，"
                f"板块内 {strong_count} 只涨超 5%，联动确认。"
            )
            events.append({
                "type": "sector_surge",
                "symbol": leader["symbol"],
                "name": theme,
                "code": "--",
                "theme": theme,
                "message": message,
                "triggered_at": now_str,
                "level": "high" if limit_count >= 2 else "medium",
            })
            self._last_sector_alert_at[theme] = now

        return events

    def _is_sector_alert_in_cooldown(self, theme: str, now: datetime) -> bool:
        last_alert_at = self._last_sector_alert_at.get(theme)
        if not last_alert_at:
            return False
        return now - last_alert_at < SECTOR_ALERT_COOLDOWN


# Singleton
monitor = IntradayMonitor()
