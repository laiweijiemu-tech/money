import json
import re
from typing import Any, Dict, List, Optional

import requests


def _session() -> requests.Session:
    session = requests.Session()
    session.trust_env = False
    session.headers.update(
        {
            "User-Agent": "Mozilla/5.0",
            "Referer": "https://finance.sina.com.cn/",
        }
    )
    return session


def _to_float(value: Any) -> Optional[float]:
    if value in (None, "", "-"):
        return None
    try:
        return float(str(value).replace(",", ""))
    except (TypeError, ValueError):
        return None


def _sina_code_to_ts_code(code: str) -> str:
    value = str(code).strip().lower()
    if value.startswith("sh"):
        return f"{value[2:]}.SH"
    if value.startswith("sz"):
        return f"{value[2:]}.SZ"
    if value.startswith("bj"):
        return f"{value[2:]}.BJ"
    return value.upper()


def _ts_code_to_sina_code(ts_code: str) -> str:
    value = str(ts_code).strip().upper()
    if "." not in value:
        if value.startswith("6"):
            return f"sh{value}"
        if value.startswith(("0", "3")):
            return f"sz{value}"
        if value.startswith(("4", "8")):
            return f"bj{value}"
        return value.lower()
    symbol, market = value.split(".", 1)
    return f"{market.lower()}{symbol}"


def load_sina_market_gainers(limit: int = 20) -> List[Dict[str, Any]]:
    session = _session()
    url = "https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeData"
    params = {
        "page": "1",
        "num": str(limit),
        "sort": "changepercent",
        "asc": "0",
        "node": "hs_a",
        "symbol": "",
        "_s_r_a": "page",
    }
    response = session.get(url, params=params, timeout=15)
    response.raise_for_status()
    rows = response.json()
    result: List[Dict[str, Any]] = []
    for item in rows:
        code = str(item.get("symbol") or item.get("code") or "").strip().lower()
        if not code:
            continue
        result.append(
            {
                "ts_code": _sina_code_to_ts_code(code),
                "symbol": str(item.get("code") or ""),
                "name": item.get("name"),
                "close": _to_float(item.get("trade")),
                "pct_chg": _to_float(item.get("changepercent")),
                "amount": _to_float(item.get("amount")),
            }
        )
    return result


def load_eastmoney_hot_rank(limit: int = 10) -> List[Dict[str, Any]]:
    session = _session()
    response = session.post(
        "https://emappdata.eastmoney.com/stockrank/getAllCurrentList",
        json={
            "appId": "appId01",
            "globalId": "786e4c21-70dc-435a-93bb-38",
            "marketType": "",
            "pageNo": 1,
            "pageSize": limit,
        },
        timeout=15,
    )
    response.raise_for_status()
    rows = response.json().get("data", [])
    result: List[Dict[str, Any]] = []
    for item in rows:
        sc = str(item.get("sc") or "").strip().upper()
        if len(sc) < 3:
            continue
        market = sc[:2]
        symbol = sc[2:]
        ts_code = f"{symbol}.{market}"
        result.append(
            {
                "rank": item.get("rk"),
                "rank_diff": item.get("rc"),
                "symbol": ts_code,
                "raw_symbol": sc,
            }
        )
    return result


def load_sina_quotes(ts_codes: List[str]) -> Dict[str, Dict[str, Any]]:
    session = _session()
    sina_codes = [_ts_code_to_sina_code(code) for code in ts_codes if str(code).strip()]
    if not sina_codes:
        return {}
    response = session.get(
        "https://hq.sinajs.cn/list=" + ",".join(sina_codes),
        headers={"Referer": "https://finance.sina.com.cn", "User-Agent": "Mozilla/5.0"},
        timeout=15,
    )
    response.raise_for_status()
    response.encoding = "gbk"
    result: Dict[str, Dict[str, Any]] = {}
    for line in response.text.splitlines():
        match = re.match(r'var hq_str_(\w+)="(.*)";', line.strip())
        if not match:
            continue
        code = match.group(1)
        values = match.group(2).split(",")
        if len(values) < 32 or not values[0]:
            continue
        ts_code = _sina_code_to_ts_code(code)
        prev_close = _to_float(values[2])
        latest = _to_float(values[3])
        pct = None
        if prev_close not in (None, 0) and latest is not None:
            pct = (latest / prev_close - 1) * 100
        result[ts_code] = {
            "name": values[0],
            "open": _to_float(values[1]),
            "pre_close": prev_close,
            "close": latest,
            "high": _to_float(values[4]),
            "low": _to_float(values[5]),
            "buy": _to_float(values[6]),
            "sell": _to_float(values[7]),
            "vol": _to_float(values[8]),
            "amount": _to_float(values[9]),
            "date": values[30],
            "time": values[31],
            "pct_chg": pct,
        }
    return result


def _load_sina_sector_text(indicator: str) -> str:
    session = _session()
    if indicator == "新浪行业":
        response = session.get("http://vip.stock.finance.sina.com.cn/q/view/newSinaHy.php", timeout=15)
    elif indicator == "概念":
        response = session.get(
            "http://money.finance.sina.com.cn/q/view/newFLJK.php",
            params={"param": "class"},
            timeout=15,
        )
    elif indicator == "行业":
        response = session.get(
            "http://money.finance.sina.com.cn/q/view/newFLJK.php",
            params={"param": "industry"},
            timeout=15,
        )
    else:
        raise ValueError(f"Unsupported indicator: {indicator}")
    response.raise_for_status()
    return response.text


def load_sina_sector_spot(indicator: str, limit: int = 10) -> List[Dict[str, Any]]:
    text = _load_sina_sector_text(indicator)
    payload = json.loads(text[text.find("{") :])
    rows: List[Dict[str, Any]] = []
    for _, raw_value in payload.items():
        parts = raw_value.split(",")
        if len(parts) < 13:
            continue
        leader_symbol = str(parts[8]).strip().lower()
        rows.append(
            {
                "label": parts[0],
                "name": parts[1],
                "count": int(float(parts[2])) if parts[2] else 0,
                "avg_price": _to_float(parts[3]),
                "change_amount": _to_float(parts[4]),
                "pct": _to_float(parts[5]),
                "volume": _to_float(parts[6]),
                "amount": _to_float(parts[7]),
                "leader_symbol": _sina_code_to_ts_code(leader_symbol) if leader_symbol else "",
                "leader_pct": _to_float(parts[9]),
                "leader_price": _to_float(parts[10]),
                "leader_change": _to_float(parts[11]),
                "leader_name": parts[12],
            }
        )
    rows.sort(key=lambda item: (item.get("pct") or -999, item.get("amount") or -1), reverse=True)
    return rows[:limit]
