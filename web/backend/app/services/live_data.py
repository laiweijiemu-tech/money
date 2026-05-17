from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime, timedelta
from threading import Lock
from typing import Any

import requests

from ..database import load_dashboard_snapshot, save_dashboard_snapshot

QUOTE_URL = "https://hq.sinajs.cn/list="
BOARD_RANKING_URL = "http://money.finance.sina.com.cn/q/view/newFLJK.php?param=class"
BOARD_STOCKS_URL = (
    "https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php"
    "/Market_Center.getHQNodeData?page=1&num={num}&sort=changepercent&asc=0"
    "&node={node}&symbol=&_s_r_a=auto"
)

CACHE_TTL_SECONDS = 12
WATCHLIST_CACHE_TTL_SECONDS = 60
TOP_BOARDS_COUNT = 4
STOCKS_PER_BOARD = 3

REQUEST_HEADERS = {
    "Referer": "https://finance.sina.com.cn",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36"
    ),
}

_cache_lock = Lock()
_cache_payload: dict[str, Any] | None = None
_cache_expires_at = datetime.min

_watchlist_lock = Lock()
_watchlist_cache: list[dict[str, Any]] | None = None
_watchlist_expires_at = datetime.min


def get_dashboard_data() -> dict:
    global _cache_payload, _cache_expires_at
    with _cache_lock:
        if _cache_payload and datetime.now() < _cache_expires_at:
            return _cache_payload

    try:
        live_payload = _build_live_dashboard()
        save_dashboard_snapshot(live_payload)
        with _cache_lock:
            _cache_payload = live_payload
            _cache_expires_at = datetime.now() + timedelta(seconds=CACHE_TTL_SECONDS)
        return live_payload
    except Exception as exc:
        fallback = load_dashboard_snapshot()
        fallback["source"] = {
            "provider": "sqlite_snapshot",
            "mode": "fallback",
            "is_live": False,
            "message": f"实时接口暂不可用，已切换本地快照：{exc}",
        }
        with _cache_lock:
            _cache_payload = fallback
            _cache_expires_at = datetime.now() + timedelta(seconds=5)
        return fallback


# ---------------------------------------------------------------------------
# Auto stock selection: hot boards → board leaders → watchlist
# ---------------------------------------------------------------------------


def _fetch_hot_boards() -> list[dict[str, Any]]:
    session = requests.Session()
    session.trust_env = False
    response = session.get(BOARD_RANKING_URL, headers=REQUEST_HEADERS, timeout=10)
    response.raise_for_status()
    response.encoding = "gbk"

    text = response.text
    start = text.index("{")
    end = text.rindex("}") + 1
    data = json.loads(text[start:end])

    boards: list[dict[str, Any]] = []
    for code, val in data.items():
        parts = val.split(",")
        if len(parts) < 13:
            continue
        name = parts[1]
        count = int(parts[2])
        avg_change = float(parts[5])
        leader_name = parts[12]
        if count < 5:
            continue
        boards.append({
            "code": code,
            "name": name,
            "count": count,
            "avg_change": avg_change,
            "leader_name": leader_name,
        })

    boards.sort(key=lambda x: x["avg_change"], reverse=True)
    return boards


def _get_top_boards_for_selection(boards: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return boards[:TOP_BOARDS_COUNT]


def _fetch_board_leaders(board_code: str, board_name: str) -> list[dict[str, Any]]:
    url = BOARD_STOCKS_URL.format(num=STOCKS_PER_BOARD * 2, node=board_code)
    session = requests.Session()
    session.trust_env = False
    response = session.get(url, headers=REQUEST_HEADERS, timeout=10)
    response.raise_for_status()
    items = response.json()

    leaders: list[dict[str, Any]] = []
    for item in items:
        symbol_raw = item.get("symbol", "")
        name = item.get("name", "")
        if symbol_raw.startswith("bj"):
            continue
        if "ST" in name or "st" in name:
            continue
        pct = float(item.get("changepercent", 0))
        amount = float(item.get("amount", 0))
        volume = float(item.get("volume", 0))
        trade = float(item.get("trade", 0))
        nmc = float(item.get("nmc", 0))
        mktcap = float(item.get("mktcap", 0))
        turnover_rate = float(item.get("turnoverratio", 0))

        # nmc is 流通市值(万元), use to estimate float_shares for turnover calc
        float_shares = int(nmc * 10000 / trade) if trade > 0 and nmc > 0 else 0

        leaders.append({
            "symbol": symbol_raw,
            "name": name,
            "theme": board_name,
            "change_pct": pct,
            "amount": amount,
            "volume": volume,
            "last_price": trade,
            "nmc": nmc,
            "mktcap": mktcap,
            "turnover_rate": turnover_rate,
            "float_shares": float_shares,
        })

        if len(leaders) >= STOCKS_PER_BOARD:
            break

    return leaders


def _assign_tags(leaders_by_board: dict[str, list[dict[str, Any]]]) -> None:
    for _board, stocks in leaders_by_board.items():
        if not stocks:
            continue
        stocks.sort(key=lambda x: x["change_pct"], reverse=True)

        top = stocks[0]
        code = top["symbol"][2:]
        threshold = 19.5 if _is_gem_or_star(code) else 9.5
        if top["change_pct"] >= threshold:
            top["tag"] = "身位龙"
        else:
            top["tag"] = "先锋"

        if len(stocks) > 1:
            by_amount = sorted(stocks[1:], key=lambda x: x["amount"], reverse=True)
            by_amount[0]["tag"] = "中军"
            for s in by_amount[1:]:
                s.setdefault("tag", "辨识度")

        for s in stocks:
            s.setdefault("tag", "辨识度")


_all_boards_cache: list[dict[str, Any]] | None = None
_all_boards_expires_at = datetime.min


def _build_watchlist() -> list[dict[str, Any]]:
    global _watchlist_cache, _watchlist_expires_at, _all_boards_cache, _all_boards_expires_at
    with _watchlist_lock:
        if _watchlist_cache and datetime.now() < _watchlist_expires_at:
            return _watchlist_cache

    all_boards = _fetch_hot_boards()
    if not all_boards:
        raise RuntimeError("未获取到热门板块数据")

    _all_boards_cache = all_boards
    _all_boards_expires_at = datetime.now() + timedelta(seconds=WATCHLIST_CACHE_TTL_SECONDS)

    hot_boards = _get_top_boards_for_selection(all_boards)

    leaders_by_board: dict[str, list[dict[str, Any]]] = {}
    seen_symbols: set[str] = set()
    all_leaders: list[dict[str, Any]] = []

    for board in hot_boards:
        stocks = _fetch_board_leaders(board["code"], board["name"])
        deduped: list[dict[str, Any]] = []
        for s in stocks:
            if s["symbol"] not in seen_symbols:
                seen_symbols.add(s["symbol"])
                deduped.append(s)
        leaders_by_board[board["name"]] = deduped
        all_leaders.extend(deduped)

    _assign_tags(leaders_by_board)

    watchlist: list[dict[str, Any]] = []
    for item in all_leaders:
        watchlist.append({
            "symbol": item["symbol"],
            "theme": item["theme"],
            "tag": item["tag"],
            "float_shares": item.get("float_shares", 0),
            "mktcap": item.get("mktcap", 0),
            "cost_price": None,
        })

    with _watchlist_lock:
        _watchlist_cache = watchlist
        _watchlist_expires_at = datetime.now() + timedelta(seconds=WATCHLIST_CACHE_TTL_SECONDS)

    return watchlist


# ---------------------------------------------------------------------------
# Dashboard building
# ---------------------------------------------------------------------------


def _format_hot_boards() -> list[dict[str, Any]]:
    boards = _all_boards_cache or []
    return [
        {
            "name": b["name"],
            "avg_change": round(b["avg_change"], 2),
            "count": b["count"],
            "leader_name": b["leader_name"],
        }
        for b in boards[:10]
    ]


def _build_live_dashboard() -> dict:
    watchlist = _build_watchlist()
    quotes = _fetch_sina_quotes(watchlist)
    if not quotes:
        raise RuntimeError("未获取到有效实时行情")

    theme_items = _build_theme_items(quotes)
    leaders = _build_leaders(quotes)
    alerts = _build_alerts(quotes, theme_items)
    positions = _build_positions(quotes)
    sentiment = _estimate_sentiment(quotes, theme_items)
    overview = _build_overview(quotes, theme_items, alerts, sentiment)
    emotion_trend = _build_emotion_trend(sentiment)

    hot_boards = _format_hot_boards()

    return {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "overview": overview,
        "emotion_trend": emotion_trend,
        "themes": theme_items,
        "hot_boards": hot_boards,
        "leaders": leaders,
        "alerts": alerts,
        "positions": positions,
        "source": {
            "provider": "sina_realtime",
            "mode": "live",
            "is_live": True,
            "message": "当前使用新浪公开行情接口实时刷新，选股池自动更新。",
        },
    }


def _fetch_sina_quotes(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    symbols = ",".join(item["symbol"] for item in items)
    session = requests.Session()
    session.trust_env = False
    response = session.get(f"{QUOTE_URL}{symbols}", headers=REQUEST_HEADERS, timeout=10)
    response.raise_for_status()
    response.encoding = "gbk"

    parsed: list[dict[str, Any]] = []
    config_map = {item["symbol"]: item for item in items}
    for line in response.text.splitlines():
        quote = _parse_sina_line(line, config_map)
        if quote:
            parsed.append(quote)
    return parsed


def _parse_sina_line(line: str, config_map: dict[str, dict[str, Any]]) -> dict[str, Any] | None:
    if not line.startswith("var hq_str_") or '="' not in line:
        return None

    prefix, raw = line.split('="', maxsplit=1)
    symbol = prefix.replace("var hq_str_", "").strip()
    payload = raw.rstrip('";')
    fields = payload.split(",")
    if len(fields) < 32 or not fields[0]:
        return None

    config = config_map.get(symbol)
    if not config:
        return None

    name = fields[0]
    open_price = _safe_float(fields[1])
    prev_close = _safe_float(fields[2])
    last_price = _safe_float(fields[3])
    high_price = _safe_float(fields[4])
    low_price = _safe_float(fields[5])
    volume = _safe_float(fields[8])
    amount = _safe_float(fields[9])
    bid1_volume = _safe_float(fields[10])
    bid1_price = _safe_float(fields[11])
    ask1_volume = _safe_float(fields[20])
    ask1_price = _safe_float(fields[21])
    trade_time = f"{fields[30]} {fields[31]}"
    change_pct = ((last_price - prev_close) / prev_close * 100) if prev_close else 0.0

    float_shares = config["float_shares"]
    turnover_rate = (volume / float_shares * 100) if float_shares else 0.0

    seal_amount = max(bid1_volume * bid1_price / 100000000, ask1_volume * ask1_price / 100000000)

    return {
        "code": symbol[2:],
        "symbol": symbol,
        "name": name,
        "theme": config["theme"],
        "tag": config["tag"],
        "last_price": last_price,
        "prev_close": prev_close,
        "open_price": open_price,
        "high_price": high_price,
        "low_price": low_price,
        "change_pct": round(change_pct, 2),
        "turnover_rate": round(turnover_rate, 2),
        "amount": amount,
        "volume": volume,
        "seal_amount": round(seal_amount, 2),
        "trade_time": trade_time,
        "cost_price": config["cost_price"],
        "float_shares": float_shares,
        "mktcap": config.get("mktcap", 0),
    }


def _build_theme_items(quotes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for quote in quotes:
        grouped[quote["theme"]].append(quote)

    theme_items = []
    for theme, members in grouped.items():
        avg_change = sum(item["change_pct"] for item in members) / len(members)
        leader_count = sum(1 for item in members if item["change_pct"] >= 5)
        heat = min(100, max(28, int(55 + avg_change * 4 + leader_count * 10 + len(members) * 2)))
        theme_items.append(
            {
                "name": theme,
                "heat": heat,
                "change_pct": round(avg_change, 2),
                "leaders": leader_count,
            }
        )

    return sorted(theme_items, key=lambda item: (-item["heat"], -item["change_pct"], item["name"]))


def _is_gem_or_star(code: str) -> bool:
    return code.startswith("30") or code.startswith("688")


def _get_limit_up_threshold(code: str) -> float:
    return 19.8 if _is_gem_or_star(code) else 9.7


def _build_leaders(quotes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    sorted_quotes = sorted(
        quotes,
        key=lambda item: (item["change_pct"], item["amount"], -item["last_price"]),
        reverse=True,
    )
    leaders = []
    for quote in sorted_quotes[:6]:
        limit_threshold = _get_limit_up_threshold(quote["code"])
        market_cap = quote.get("mktcap", 0) / 10000
        leaders.append(
            {
                "code": quote["code"],
                "name": quote["name"],
                "theme": quote["theme"],
                "tag": quote["tag"],
                "last_price": quote["last_price"],
                "change_pct": quote["change_pct"],
                "turnover_rate": quote["turnover_rate"],
                "seal_amount": quote["seal_amount"],
                "market_cap": round(market_cap, 2),
                "status": _describe_status(quote["change_pct"], quote["last_price"], quote["high_price"], quote["code"]),
            }
        )
    return leaders


def _build_alerts(quotes: list[dict[str, Any]], themes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    alerts = []
    for quote in sorted(quotes, key=lambda item: item["change_pct"], reverse=True):
        if quote["change_pct"] >= 9.7:
            alerts.append(
                {
                    "title": "强势封板",
                    "stock_name": quote["name"],
                    "stock_code": quote["code"],
                    "level": "high",
                    "message": f"涨幅 {quote['change_pct']}%，当前处于强势涨停区间。",
                    "triggered_at": quote["trade_time"].split()[-1],
                }
            )
        elif quote["change_pct"] >= 6:
            alerts.append(
                {
                    "title": "加速拉升",
                    "stock_name": quote["name"],
                    "stock_code": quote["code"],
                    "level": "medium",
                    "message": f"涨幅 {quote['change_pct']}%，换手 {quote['turnover_rate']}%，关注回封机会。",
                    "triggered_at": quote["trade_time"].split()[-1],
                }
            )

    for theme in themes[:2]:
        if theme["change_pct"] >= 3 and theme["leaders"] >= 1:
            alerts.append(
                {
                    "title": "板块联动",
                    "stock_name": theme["name"],
                    "stock_code": "--",
                    "level": "medium",
                    "message": f"{theme['name']} 平均涨幅 {theme['change_pct']}%，热度 {theme['heat']}。",
                    "triggered_at": datetime.now().strftime("%H:%M:%S"),
                }
            )

    return alerts[:6]


def _build_positions(quotes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    positions = []
    for quote in quotes:
        cost_price = quote["cost_price"]
        if cost_price is None:
            continue
        profit_pct = ((quote["last_price"] - cost_price) / cost_price * 100) if cost_price else 0.0
        drawdown_pct = ((quote["high_price"] - quote["last_price"]) / quote["high_price"] * 100) if quote["high_price"] else 0.0
        positions.append(
            {
                "code": quote["code"],
                "name": quote["name"],
                "cost_price": round(cost_price, 2),
                "last_price": quote["last_price"],
                "profit_pct": round(profit_pct, 2),
                "drawdown_pct": round(drawdown_pct, 2),
                "risk_note": _build_risk_note(profit_pct, drawdown_pct),
            }
        )
    return positions


def _build_overview(
    quotes: list[dict[str, Any]],
    themes: list[dict[str, Any]],
    alerts: list[dict[str, Any]],
    sentiment: int,
) -> dict[str, Any]:
    rising_count = sum(1 for quote in quotes if quote["change_pct"] > 0)
    strong_count = sum(1 for quote in quotes if quote["change_pct"] >= 5)
    board_success_rate = round(strong_count / max(1, len(quotes)) * 100, 1)
    limit_up_premium = round(sum(item["change_pct"] for item in sorted(quotes, key=lambda q: q["change_pct"], reverse=True)[:2]) / 2, 2)
    highest_board = 3 if any(item["change_pct"] >= 9.7 for item in quotes) else 2 if strong_count >= 2 else 1
    return {
        "trading_phase": _resolve_trading_phase(),
        "main_theme": themes[0]["name"] if themes else "暂无主线",
        "market_sentiment": sentiment,
        "watch_count": len(quotes),
        "alert_count": len(alerts),
        "board_success_rate": board_success_rate,
        "limit_up_premium": limit_up_premium,
        "highest_board": highest_board,
        "rising_count": rising_count,
    }


def _estimate_sentiment(quotes: list[dict[str, Any]], themes: list[dict[str, Any]]) -> int:
    positive_ratio = sum(1 for item in quotes if item["change_pct"] > 0) / max(1, len(quotes))
    strong_ratio = sum(1 for item in quotes if item["change_pct"] >= 5) / max(1, len(quotes))
    theme_bonus = themes[0]["heat"] / 5 if themes else 0
    score = 35 + positive_ratio * 25 + strong_ratio * 25 + theme_bonus
    return max(20, min(95, int(score)))


def _build_emotion_trend(sentiment: int) -> list[dict[str, Any]]:
    trend = [
        max(20, sentiment - 16),
        max(25, sentiment - 10),
        max(30, sentiment - 5),
        max(25, sentiment - 2),
        sentiment,
    ]
    labels = ["周一", "周二", "周三", "周四", "周五"]
    return [{"label": label, "value": value} for label, value in zip(labels, trend, strict=True)]


def _resolve_trading_phase() -> str:
    current = datetime.now().time()
    if current.hour < 9 or (current.hour == 9 and current.minute < 25):
        return "盘前准备"
    if (current.hour == 11 and current.minute > 30) or (12 <= current.hour < 13):
        return "午间观察"
    if current.hour < 15:
        return "盘中监控"
    return "盘后复盘"


def _describe_status(change_pct: float, last_price: float, high_price: float, code: str) -> str:
    limit_threshold = _get_limit_up_threshold(code)
    if change_pct >= limit_threshold:
        return "强势封板"
    if change_pct >= limit_threshold * 0.6:
        return "加速拉升"
    if high_price and abs(last_price - high_price) / high_price < 0.01:
        return "临近分时高点"
    if change_pct > 0:
        return "板块联动"
    return "震荡观察"


def _build_risk_note(profit_pct: float, drawdown_pct: float) -> str:
    if drawdown_pct >= 4:
        return "回撤偏大，跌破分时承接位建议减仓。"
    if profit_pct >= 8:
        return "浮盈较厚，可结合量能强弱做分批止盈。"
    if profit_pct > 0:
        return "仍处盈利区间，留意午后承接是否转弱。"
    return "暂未脱离成本区，注意控制单票风险。"


def _safe_float(value: str) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0
