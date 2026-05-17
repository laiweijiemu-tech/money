from __future__ import annotations

import logging
import threading
import time
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any

import requests

logger = logging.getLogger(__name__)

MARKET_TOP_URL = (
    "https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php"
    "/Market_Center.getHQNodeData?page=1&num=50&sort=changepercent&asc=0"
    "&node=hs_a&symbol=&_s_r_a=auto"
)
REQUEST_HEADERS = {
    "Referer": "https://finance.sina.com.cn",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36"
    ),
}

TICK_INTERVAL = 10
MAX_EVENTS = 50
MAX_LIVE_POOL = 20


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


def _limit_threshold(code: str) -> float:
    if code.startswith("30") or code.startswith("688"):
        return 19.5
    return 9.5


class StockState:
    __slots__ = (
        "symbol", "name", "code", "theme", "change_pct", "open_change_pct",
        "status", "prev_status", "first_limit_time", "break_count",
        "amount", "seal_amount",
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


class IntradayMonitor:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._states: dict[str, StockState] = {}
        self._events: list[dict[str, Any]] = []
        self._live_pool: dict[str, dict[str, Any]] = {}
        self._sector_history: dict[str, list[tuple[datetime, float]]] = defaultdict(list)
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
                            self._sector_history.clear()
            except Exception as exc:
                logger.warning(f"Monitor tick error: {exc}")
            time.sleep(TICK_INTERVAL)

    def _tick(self) -> None:
        stocks = self._fetch_top_stocks()
        if not stocks:
            return

        now_str = datetime.now().strftime("%H:%M:%S")
        new_events: list[dict[str, Any]] = []

        with self._lock:
            for item in stocks:
                symbol = item["symbol"]
                code = item["code"]
                name = item["name"]
                pct = item["change_pct"]
                theme = item.get("theme", "")

                state = self._states.get(symbol)
                if not state:
                    state = StockState(symbol, name, code, theme)
                    self._states[symbol] = state

                state.prev_status = state.status
                state.change_pct = pct
                state.amount = item.get("amount", 0)

                if state.open_change_pct is None:
                    state.open_change_pct = pct

                threshold = _limit_threshold(code)
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
                        state.break_count += 1
                        event = {
                            "type": "limit_up_reseal",
                            "symbol": symbol,
                            "name": name,
                            "code": code,
                            "theme": theme,
                            "message": f"炸板回封（第{state.break_count}次），涨幅 {pct:.1f}%",
                            "triggered_at": now_str,
                            "level": "high",
                        }
                        new_events.append(event)
                    else:
                        # 新封板
                        state.first_limit_time = now_str
                        is_weak_to_strong = (
                            state.open_change_pct is not None
                            and state.open_change_pct < 2.0
                        )
                        if is_weak_to_strong:
                            event = {
                                "type": "weak_to_strong",
                                "symbol": symbol,
                                "name": name,
                                "code": code,
                                "theme": theme,
                                "message": f"弱转强封板！开盘涨幅仅 {state.open_change_pct:.1f}%，现已涨停 {pct:.1f}%",
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
                                "message": f"盘中封板，涨幅 {pct:.1f}%，成交 {state.amount / 1e8:.1f}亿",
                                "triggered_at": now_str,
                                "level": "high",
                            }
                        new_events.append(event)

                    # Add to live pool
                    if symbol not in self._live_pool and len(self._live_pool) < MAX_LIVE_POOL:
                        self._live_pool[symbol] = {
                            "symbol": symbol,
                            "theme": theme or "盘中捕获",
                            "tag": "盘中捕获",
                            "float_shares": 0,
                            "mktcap": 0,
                            "cost_price": None,
                        }

                elif state.prev_status == "limit_up" and new_status == "broken":
                    state.break_count += 1

                # Track sector changes for sector surge detection
                if theme:
                    self._sector_history[theme].append((datetime.now(), pct))

            # Sector surge detection
            sector_events = self._detect_sector_surge(now_str)
            new_events.extend(sector_events)

            # Append events
            self._events.extend(new_events)
            if len(self._events) > MAX_EVENTS:
                self._events = self._events[-MAX_EVENTS:]

    def _detect_sector_surge(self, now_str: str) -> list[dict[str, Any]]:
        events: list[dict[str, Any]] = []
        cutoff = datetime.now() - timedelta(minutes=5)

        sector_counts: dict[str, int] = defaultdict(int)
        sector_limit_counts: dict[str, int] = defaultdict(int)

        for symbol, state in self._states.items():
            if not state.theme:
                continue
            if state.status == "limit_up":
                sector_limit_counts[state.theme] += 1
            if state.change_pct >= 5:
                sector_counts[state.theme] += 1

        for theme, count in sector_counts.items():
            if count >= 3 and sector_limit_counts.get(theme, 0) >= 2:
                # Avoid duplicate sector alerts within 5 minutes
                recent_sector_events = [
                    e for e in self._events
                    if e["type"] == "sector_surge" and e["theme"] == theme
                ]
                if recent_sector_events:
                    last_time = recent_sector_events[-1]["triggered_at"]
                    # Simple dedup: skip if last alert was recent
                    if last_time >= (datetime.now() - timedelta(minutes=5)).strftime("%H:%M:%S"):
                        continue

                events.append({
                    "type": "sector_surge",
                    "symbol": "--",
                    "name": theme,
                    "code": "--",
                    "theme": theme,
                    "message": f"{theme} 板块联动！{sector_limit_counts[theme]}只涨停，{count}只涨超5%",
                    "triggered_at": now_str,
                    "level": "medium",
                })

        return events

    def _fetch_top_stocks(self) -> list[dict[str, Any]]:
        try:
            session = requests.Session()
            session.trust_env = False
            response = session.get(MARKET_TOP_URL, headers=REQUEST_HEADERS, timeout=10)
            response.raise_for_status()
            items = response.json()

            results: list[dict[str, Any]] = []
            for item in items:
                symbol = item.get("symbol", "")
                name = item.get("name", "")
                if symbol.startswith("bj"):
                    continue
                if "ST" in name or "st" in name:
                    continue
                code = item.get("code", "")
                pct = float(item.get("changepercent", 0))
                amount = float(item.get("amount", 0))

                results.append({
                    "symbol": symbol,
                    "code": code,
                    "name": name,
                    "change_pct": pct,
                    "amount": amount,
                    "theme": "",
                })

            return results
        except Exception as exc:
            logger.warning(f"Fetch top stocks failed: {exc}")
            return []


# Singleton
monitor = IntradayMonitor()
