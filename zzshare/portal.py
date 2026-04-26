import os
import re
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Any, Dict, List, Optional

import akshare as ak
import pandas as pd
from flask import Flask, redirect, render_template, request, url_for

from zzshare.client import DataApi
from zzshare.direct_sources import (
    load_eastmoney_hot_rank,
    load_sina_market_gainers,
    load_sina_quotes,
    load_sina_sector_spot,
)
from zzshare.doubao import DoubaoClient, DoubaoConfigError, DoubaoRequestError, is_doubao_configured


def create_app() -> Flask:
    app = Flask(__name__, template_folder="templates", static_folder="static")

    @app.template_filter("cn_num")
    def cn_num(value: Any) -> str:
        if value in (None, "", "-"):
            return "-"
        try:
            number = float(value)
        except (TypeError, ValueError):
            return str(value)
        abs_number = abs(number)
        if abs_number >= 100000000:
            return f"{number / 100000000:.2f}亿"
        if abs_number >= 10000:
            return f"{number / 10000:.2f}万"
        if float(number).is_integer():
            return str(int(number))
        return f"{number:.2f}"

    @app.template_filter("pct")
    def pct(value: Any) -> str:
        if value in (None, ""):
            return "-"
        try:
            number = float(value)
        except (TypeError, ValueError):
            return str(value)
        return f"{number:.2f}%"

    @app.context_processor
    def inject_globals() -> Dict[str, Any]:
        return {
            "portal_title": "ZZShare Stock Portal",
            "token_configured": bool(os.getenv("ZZSHARE_TOKEN")),
            "doubao_configured": is_doubao_configured(),
        }

    @app.get("/")
    def index():
        latest_trade_date = datetime.now().strftime("%Y%m%d")
        market_snapshot = _load_market_snapshot_akshare()
        hot_stocks = _load_hot_stocks_akshare(market_snapshot, limit=10)
        industry_flows = _load_industry_flows_akshare(10)
        hot_boards, hot_boards_trade_date = _load_hot_boards_akshare(8)

        market_summary = _build_market_summary(market_snapshot, latest_trade_date)
        top_movers = _top_movers(market_snapshot, limit=8)
        hot_boards_hint = _build_hot_boards_hint(latest_trade_date, hot_boards_trade_date, hot_boards)

        return render_template(
            "index.html",
            latest_trade_date=latest_trade_date,
            latest_trade_date_display=_display_trade_date(latest_trade_date),
            hot_boards_trade_date_display=_display_trade_date(hot_boards_trade_date),
            industry_flows_trade_date_display=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            market_summary=market_summary,
            top_movers=top_movers,
            hot_stocks=hot_stocks,
            industry_flows=industry_flows,
            hot_boards=hot_boards,
            hot_boards_hint=hot_boards_hint,
        )

    @app.get("/leader-strategy")
    def leader_strategy():
        now = datetime.now()
        latest_trade_date = now.strftime("%Y%m%d")
        requested_backtest_date = request.args.get("backtest_date")
        backtest_date = _normalize_backtest_date(requested_backtest_date)
        prev_backtest_date = _shift_backtest_date(backtest_date, -1)
        next_backtest_date = _shift_backtest_date(backtest_date, 1)
        if _is_premarket_mode(now):
            reference_date = _get_previous_trade_date(latest_trade_date)
            premarket = _build_premarket_context(reference_date, candidate_limit=12)
            market_snapshot = premarket["market_snapshot"]
            hot_stocks = premarket["hot_stocks"]
            industry_flows = premarket["industry_flows"]
            hot_boards = premarket["hot_boards"]
            hot_boards_trade_date = f"{premarket['reference_date_display']} 盘前复盘"
            candidates = premarket["candidates"]
            rejected_candidates = premarket["rejected_candidates"]
        else:
            market_snapshot = _load_market_snapshot_akshare()
            hot_stocks = _load_hot_stocks_akshare(market_snapshot, limit=20)
            industry_flows = _load_industry_flows_akshare(12)
            hot_boards, hot_boards_trade_date = _load_hot_boards_akshare(12)
            candidates = _build_leader_strategy_candidates(
                market_snapshot=market_snapshot,
                hot_stocks=hot_stocks,
                industry_flows=industry_flows,
                hot_boards=hot_boards,
                limit=12,
            )
            rejected_candidates = _build_leader_strategy_rejected_candidates(
                market_snapshot=market_snapshot,
                hot_stocks=hot_stocks,
                industry_flows=industry_flows,
                hot_boards=hot_boards,
                limit=12,
            )
        rules = _get_leader_strategy_rules()
        backtest = _empty_leader_strategy_backtest(backtest_date)
        if requested_backtest_date:
            backtest = _build_leader_strategy_backtest(backtest_date, limit=10)

        return render_template(
            "leader_strategy.html",
            latest_trade_date_display=_display_trade_date(latest_trade_date),
            hot_boards_trade_date_display=hot_boards_trade_date,
            now_display=now.strftime("%Y-%m-%d %H:%M:%S"),
            time_window_status=_build_time_window_status(now),
            backtest_date=backtest_date,
            prev_backtest_date=prev_backtest_date,
            next_backtest_date=next_backtest_date,
            rules=rules,
            candidates=candidates,
            rejected_candidates=rejected_candidates,
            hot_stocks=hot_stocks[:6],
            hot_boards=hot_boards[:6],
            industry_flows=industry_flows[:6],
            backtest=backtest,
        )

    @app.get("/tomorrow-outlook")
    def tomorrow_outlook():
        now = datetime.now()
        latest_trade_date = now.strftime("%Y%m%d")
        if _is_premarket_mode(now):
            reference_date = _get_previous_trade_date(latest_trade_date)
            premarket = _build_premarket_context(reference_date, candidate_limit=8)
            market_snapshot = premarket["market_snapshot"]
            hot_stocks = premarket["hot_stocks"]
            industry_flows = premarket["industry_flows"]
            hot_boards = premarket["hot_boards"]
            hot_boards_trade_date = f"{premarket['reference_date_display']} 盘前复盘"
            candidates = premarket["candidates"]
            outlook = _build_premarket_outlook(reference_date, candidates)
        else:
            market_snapshot = _load_market_snapshot_akshare()
            hot_stocks = _load_hot_stocks_akshare(market_snapshot, limit=20)
            industry_flows = _load_industry_flows_akshare(10)
            hot_boards, hot_boards_trade_date = _load_hot_boards_akshare(10)
            candidates = _build_leader_strategy_candidates(
                market_snapshot=market_snapshot,
                hot_stocks=hot_stocks,
                industry_flows=industry_flows,
                hot_boards=hot_boards,
                limit=8,
            )
            outlook = _build_tomorrow_outlook(
                market_snapshot=market_snapshot,
                hot_stocks=hot_stocks,
                industry_flows=industry_flows,
                hot_boards=hot_boards,
                candidates=candidates,
            )

        return render_template(
            "tomorrow_outlook.html",
            latest_trade_date_display=_display_trade_date(latest_trade_date),
            hot_boards_trade_date_display=hot_boards_trade_date,
            now_display=now.strftime("%Y-%m-%d %H:%M:%S"),
            outlook=outlook,
        )

    @app.route("/stock", methods=["GET", "POST"])
    def stock_search():
        source = request.form if request.method == "POST" else request.args
        query = (source.get("q") or "").strip()
        selected_symbol = (source.get("symbol") or "").strip()
        ai_advice = None
        ai_error = None
        if not query:
            return render_template(
                "stock.html",
                query="",
                results=[],
                stock=None,
                chart=[],
                intraday_chart=[],
                latest_quote=None,
                error=None,
                ai_advice=None,
                ai_error=None,
            )

        api = DataApi()
        results = _search_stocks(api, query)
        selected = _choose_stock(results, selected_symbol)

        if selected is None:
            return render_template(
                "stock.html",
                query=query,
                results=[],
                stock=None,
                chart=[],
                intraday_chart=[],
                latest_quote=None,
                error="没有找到匹配的股票，请尝试输入代码或简称。",
                ai_advice=None,
                ai_error=None,
            )

        with ThreadPoolExecutor(max_workers=3) as executor:
            chart_future = executor.submit(_daily_chart, api, selected["ts_code"])
            intraday_chart_future = executor.submit(_intraday_chart, selected["ts_code"])
            latest_quote_future = executor.submit(_latest_quote, api, selected["ts_code"])
            chart = chart_future.result()
            intraday_chart = intraday_chart_future.result()
            latest_quote = latest_quote_future.result()
        if request.method == "POST":
            ai_advice, ai_error = _generate_stock_advice(selected, chart, latest_quote)

        return render_template(
            "stock.html",
            query=query,
            results=results,
            stock=selected,
            chart=chart,
            intraday_chart=intraday_chart,
            latest_quote=latest_quote,
            error=None,
            ai_advice=ai_advice,
            ai_error=ai_error,
        )

    @app.get("/stock/<symbol>")
    def stock_redirect(symbol: str):
        return redirect(url_for("stock_search", q=symbol, symbol=symbol))

    return app


def _safe_call(func, *args, **kwargs):
    try:
        return func(*args, **kwargs)
    except Exception:
        return None


def _get_local_zzshare_token() -> str:
    token = os.getenv("ZZSHARE_TOKEN", "").strip()
    if token:
        return token

    test_file = os.path.join(os.getcwd(), "test_zzshare.py")
    if not os.path.exists(test_file):
        return ""

    try:
        content = open(test_file, "r", encoding="utf-8").read()
    except OSError:
        return ""

    match = re.search(r'ZZSHARE_TOKEN", "([0-9a-f]+)"', content)
    if match:
        return match.group(1)
    return ""


def _build_local_zzshare_api() -> Optional[DataApi]:
    token = _get_local_zzshare_token()
    if not token:
        return None
    return DataApi(token=token)


def _get_latest_trade_date(api: DataApi) -> str:
    trade_days = _safe_call(api.trade_days, days=10)
    if isinstance(trade_days, list) and trade_days:
        value = str(trade_days[-1]).replace("-", "")
        if len(value) == 8:
            return value
    return datetime.now().strftime("%Y%m%d")


def _display_trade_date(trade_date: str) -> str:
    value = str(trade_date or "").replace("-", "")
    if len(value) != 8 or not value.isdigit():
        return str(trade_date)
    return f"{value[:4]}-{value[4:6]}-{value[6:]}"


def _build_hot_boards_hint(
    latest_trade_date: str,
    hot_boards_trade_date: str,
    hot_boards: List[Dict[str, Any]],
) -> Dict[str, str]:
    latest_display = _display_trade_date(latest_trade_date)
    hot_boards_display = hot_boards_trade_date
    if hot_boards:
        return {
            "tone": "neutral",
            "message": f"当前展示新浪概念板块异动榜，更新时间 {hot_boards_display}。",
        }
    return {
        "tone": "warning",
        "message": (
            f"当前未获取到新浪概念板块数据。最近交易日为 {latest_display}。"
            "这通常是网络波动、源站限流，或当前环境访问受限。"
        ),
    }


def _load_hot_boards_akshare(limit: int = 8) -> tuple[List[Dict[str, Any]], str]:
    try:
        rows = load_sina_sector_spot(indicator="概念", limit=limit)
    except Exception:
        return [], datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    if not rows:
        return [], datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    result = []
    for row in rows:
        result.append(
            {
                "name": row.get("name"),
                "count": row.get("count"),
                "leader": row.get("leader_name"),
                "leader_symbol": row.get("leader_symbol"),
                "reason": (
                    f"板块涨跌幅 {row.get('pct')}%，"
                    f"总成交额 {row.get('amount')}，"
                    f"领涨股涨跌幅 {row.get('leader_pct')}%"
                ),
            }
        )
    return result, datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _load_market_snapshot_akshare(limit: int = 150) -> pd.DataFrame:
    try:
        rows = load_sina_market_gainers(limit=limit)
    except Exception:
        return pd.DataFrame(columns=["ts_code", "name", "close", "pct_chg", "amount", "timestamp"])

    if not rows:
        return pd.DataFrame(columns=["ts_code", "name", "close", "pct_chg", "amount", "timestamp"])

    snapshot = pd.DataFrame(rows)
    snapshot["timestamp"] = datetime.now().strftime("%H:%M:%S")
    return snapshot[["ts_code", "name", "close", "pct_chg", "amount", "timestamp"]]


def _load_hot_stocks_akshare(market_snapshot: pd.DataFrame, limit: int = 10) -> List[Dict[str, Any]]:
    try:
        rank_rows = load_eastmoney_hot_rank(limit=limit)
    except Exception:
        return []

    if not rank_rows:
        return []

    quote_map = load_sina_quotes([row["symbol"] for row in rank_rows])
    rows: List[Dict[str, Any]] = []
    for row in rank_rows:
        quote = quote_map.get(row["symbol"], {})
        rows.append(
            {
                "rank": row.get("rank"),
                "symbol": row.get("symbol"),
                "name": quote.get("name") or row.get("symbol"),
                "price": quote.get("close"),
                "pct": quote.get("pct_chg"),
                "rank_diff": row.get("rank_diff"),
            }
        )
    return rows


def _load_industry_flows_akshare(limit: int = 10) -> List[Dict[str, Any]]:
    try:
        rows = load_sina_sector_spot(indicator="行业", limit=limit)
    except Exception:
        return []

    if not rows:
        return []

    result: List[Dict[str, Any]] = []
    for row in rows:
        result.append(
            {
                "name": row.get("name"),
                "pct": row.get("pct"),
                "leader": row.get("leader_name"),
                "leader_symbol": row.get("leader_symbol"),
                "net_inflow": row.get("amount"),
                "leader_pct": row.get("leader_pct"),
            }
        )
    return result


def _normalize_xq_symbol_to_ts_code(symbol: Any) -> str:
    value = str(symbol).strip().upper()
    if len(value) < 8:
        return value
    market = value[:2]
    code = value[2:]
    if market == "SH":
        return f"{code}.SH"
    if market == "SZ":
        return f"{code}.SZ"
    if market == "BJ":
        return f"{code}.BJ"
    return value


def _parse_percent_string(value: Any) -> Optional[float]:
    if value in (None, "", "-"):
        return None
    text = str(value).strip().replace("%", "")
    try:
        return float(text)
    except ValueError:
        return None


def _parse_cn_amount_string(value: Any) -> Optional[float]:
    if value in (None, "", "-"):
        return None
    text = str(value).strip().replace(",", "")
    multiplier = 1.0
    if text.endswith("亿"):
        multiplier = 100000000.0
        text = text[:-1]
    elif text.endswith("万"):
        multiplier = 10000.0
        text = text[:-1]
    try:
        return float(text) * multiplier
    except ValueError:
        return None


def _get_leader_strategy_rules() -> List[Dict[str, str]]:
    return [
        {
            "title": "只看强主线",
            "content": "9:30-10:00 先确认最强题材和最强行业，优先做资金流居前、涨幅靠前的主线，不做边缘题材。",
        },
        {
            "title": "只做题材龙头",
            "content": "优先选热门题材的领涨股、行业领涨股，以及同时出现在市场热度榜里的个股，不做后排跟风。",
        },
        {
            "title": "先强度后价格",
            "content": "先看涨幅、净流入、成交额和热度，再看价格与仓位，不因为股价便宜就降低标准。",
        },
        {
            "title": "开盘半小时定方向",
            "content": "9:30-10:00 的任务是确认今天谁最强，不是抄底。强者继续强才是龙头战法的核心。",
        },
        {
            "title": "回避杂毛与风险股",
            "content": "ST、无成交额支撑、涨幅弱、没有题材共振、没有资金净流入的个股，默认排除。",
        },
        {
            "title": "候选股要有共振",
            "content": "理想龙头应同时满足题材/行业共振，并且 10:00 前涨幅足够强。热度只做辅助，不允许单靠热度入池。",
        },
    ]


def _normalize_backtest_date(value: Optional[str]) -> str:
    text = str(value or "").strip().replace("-", "")
    if len(text) == 8 and text.isdigit():
        return text
    return "20260417"


def _empty_leader_strategy_backtest(backtest_date: str) -> Dict[str, Any]:
    return {
        "date": backtest_date,
        "date_display": _display_trade_date(backtest_date),
        "rows": [],
        "summary": [],
        "insights": [],
        "top_pick": None,
        "error": None,
        "note": "默认不自动执行历史回测。输入日期后点击“回测这一天”，再计算 09:30-10:00 的龙头预测结果。",
    }


def _shift_backtest_date(date_text: str, days: int) -> str:
    try:
        dt = datetime.strptime(date_text, "%Y%m%d")
    except ValueError:
        dt = datetime.strptime("20260417", "%Y%m%d")
    shifted = dt + pd.Timedelta(days=days)
    return shifted.strftime("%Y%m%d")


def _display_limit_time(value: Any) -> str:
    text = str(value or "").strip()
    if len(text) != 6 or not text.isdigit():
        return text or "-"
    return f"{text[:2]}:{text[2:4]}:{text[4:]}"


def _build_time_window_status(now: datetime) -> Dict[str, str]:
    hhmm = now.strftime("%H:%M")
    if hhmm < "09:15":
        return {
            "tone": "neutral",
            "message": f"当前时间 {hhmm}，处于盘前模式，优先展示上一交易日复盘结果与预案。",
        }
    if "09:30" <= hhmm <= "10:00":
        return {
            "tone": "neutral",
            "message": f"当前时间 {hhmm}，处于龙头战法的核心观察窗口。",
        }
    if hhmm < "09:30":
        return {
            "tone": "warning",
            "message": f"当前时间 {hhmm}，尚未到 09:30，页面更适合做预案而非实盘决策。",
        }
    return {
        "tone": "warning",
        "message": f"当前时间 {hhmm}，已经超过 10:00，页面更适合做盘中复盘与龙头确认。",
    }


def _is_premarket_mode(now: datetime) -> bool:
    return now.strftime("%H:%M") < "09:15"


def _get_previous_trade_date(reference_date: str) -> str:
    api = _build_local_zzshare_api()
    if api is not None:
        trade_days = _safe_call(api.trade_days, day_end=reference_date, days=10)
        if isinstance(trade_days, list):
            normalized_days = sorted(
                {
                    str(item).replace("-", "")[:8]
                    for item in trade_days
                    if len(str(item).replace("-", "")) >= 8
                }
            )
            previous_days = [item for item in normalized_days if item < reference_date]
            if previous_days:
                return previous_days[-1]

    try:
        current = datetime.strptime(reference_date, "%Y%m%d")
    except ValueError:
        current = datetime.now()

    while True:
        current = current - pd.Timedelta(days=1)
        if current.weekday() < 5:
            return current.strftime("%Y%m%d")


def _make_score_tags(score_items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [
        {
            "label": item["label"],
            "score": int(item["score"]),
            "tone": "negative" if int(item["score"]) < 0 else "positive",
        }
        for item in score_items
    ]


def _build_premarket_candidates(reference_date: str, limit: int = 12) -> List[Dict[str, Any]]:
    backtest = _build_leader_strategy_backtest(reference_date, limit=max(limit, 20))
    rows = backtest.get("rows") or []
    if rows:
        candidates: List[Dict[str, Any]] = []
        for index, row in enumerate(rows[:limit], start=1):
            score = int(row.get("score", 0))
            industry = str(row.get("industry") or "").strip()
            score_items = [
                {"label": "昨日复盘强度", "score": score},
            ]
            if float(row.get("pct_1000") or 0) >= 9:
                score_items.append({"label": "10点前接近板位", "score": 12})
            if float(row.get("amount_1000") or 0) >= 500000000:
                score_items.append({"label": "早盘成交活跃", "score": 8})

            candidates.append(
                {
                    "rank": index,
                    "symbol": row.get("symbol"),
                    "name": row.get("name"),
                    "price": row.get("final_limit_price") or row.get("price_1000"),
                    "pct": row.get("final_day_pct"),
                    "amount": row.get("amount_1000"),
                    "score": score,
                    "signals": [row.get("signals_text") or "昨日复盘强势样本"],
                    "score_items": score_items,
                    "score_tags": _make_score_tags(score_items),
                    "board_name": industry or "昨日强势股",
                    "industry_name": industry or "-",
                    "entry_reason": (
                        f"盘前模式：基于 {_display_trade_date(reference_date)} 的复盘结果，"
                        "优先观察昨天 10:00 前就具备强度、且后续成功涨停的个股。"
                    ),
                    "score_breakdown": row.get("signals_text") or "昨日复盘命中强势样本",
                    "signals_text": row.get("signals_text") or "昨日复盘强势样本",
                    "rejected_reasons": [],
                    "rejected_reason_text": "",
                }
            )
        return candidates

    try:
        zt_df = ak.stock_zt_pool_em(date=reference_date)
    except Exception:
        return []

    if not isinstance(zt_df, pd.DataFrame) or zt_df.empty:
        return []

    work_df = zt_df.copy()
    work_df["代码"] = work_df["代码"].astype(str).str.zfill(6)
    work_df["首次封板时间_num"] = pd.to_numeric(work_df.get("首次封板时间"), errors="coerce")
    work_df["涨跌幅_num"] = pd.to_numeric(work_df.get("涨跌幅"), errors="coerce")
    work_df["最新价_num"] = pd.to_numeric(work_df.get("最新价"), errors="coerce")
    work_df["流通市值_num"] = pd.to_numeric(work_df.get("流通市值"), errors="coerce")

    fallback_candidates: List[Dict[str, Any]] = []
    for row in work_df.to_dict(orient="records"):
        code = str(row.get("代码", "")).zfill(6)
        name = str(row.get("名称", "")).strip()
        if not code or not name:
            continue

        pct = float(row.get("涨跌幅_num") or 0)
        limit_time = int(row.get("首次封板时间_num") or 999999)
        float_cap = float(row.get("流通市值_num") or 0)
        industry = str(row.get("所属行业") or "").strip()
        score_items: List[Dict[str, Any]] = []
        score = 0

        if pct >= 19:
            score += 30
            score_items.append({"label": "高弹性涨停", "score": 30})
        elif pct >= 9.5:
            score += 24
            score_items.append({"label": "昨日涨停", "score": 24})

        if limit_time <= 93500:
            score += 18
            score_items.append({"label": "首封较早", "score": 18})
        elif limit_time <= 100000:
            score += 14
            score_items.append({"label": "早盘封板", "score": 14})
        elif limit_time <= 110000:
            score += 8
            score_items.append({"label": "上午完成封板", "score": 8})
        else:
            score += 4
            score_items.append({"label": "午后封板", "score": 4})

        if industry:
            score += 6
            score_items.append({"label": f"所属行业 {industry}", "score": 6})

        if 0 < float_cap <= 30000000000:
            score += 4
            score_items.append({"label": "流通盘适中", "score": 4})

        fallback_candidates.append(
            {
                "rank": 0,
                "symbol": _candidate_ts_codes(code)[0],
                "name": name,
                "price": row.get("最新价_num"),
                "pct": pct,
                "amount": None,
                "score": score,
                "signals": [item["label"] for item in score_items],
                "score_items": score_items,
                "score_tags": _make_score_tags(score_items),
                "board_name": industry or "昨日涨停股",
                "industry_name": industry or "-",
                "entry_reason": (
                    f"盘前模式：基于 {_display_trade_date(reference_date)} 的涨停池复盘，"
                    "优先展示首封较早、强度明确的昨日涨停股。"
                ),
                "score_breakdown": "；".join(item["label"] for item in score_items),
                "signals_text": "；".join(item["label"] for item in score_items[:4]),
                "rejected_reasons": [],
                "rejected_reason_text": "",
            }
        )

    fallback_candidates.sort(
        key=lambda item: (
            item.get("score", 0),
            item.get("pct") if isinstance(item.get("pct"), (int, float)) else -999,
        ),
        reverse=True,
    )
    for index, item in enumerate(fallback_candidates[:limit], start=1):
        item["rank"] = index
    return fallback_candidates[:limit]


def _build_premarket_hot_lists(candidates: List[Dict[str, Any]]) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    hot_stocks = [
        {
            "rank": index,
            "symbol": row.get("symbol"),
            "name": row.get("name"),
            "price": row.get("price"),
            "pct": row.get("pct"),
            "rank_diff": None,
        }
        for index, row in enumerate(candidates[:6], start=1)
    ]

    group_stats: Dict[str, Dict[str, Any]] = {}
    for row in candidates:
        group_name = str(row.get("board_name") or row.get("industry_name") or "").strip()
        if not group_name or group_name == "-":
            continue
        stat = group_stats.setdefault(
            group_name,
            {"name": group_name, "count": 0, "leader": row.get("name"), "score": 0, "pct_sum": 0.0},
        )
        stat["count"] += 1
        stat["score"] += int(row.get("score", 0))
        stat["pct_sum"] += float(row.get("pct") or 0)
        if int(row.get("score", 0)) > stat["score"]:
            stat["leader"] = row.get("name")

    grouped = sorted(group_stats.values(), key=lambda item: (item["count"], item["score"]), reverse=True)
    hot_boards = [
        {
            "name": item["name"],
            "count": item["count"],
            "leader": item["leader"],
            "leader_symbol": "",
            "reason": f"盘前复盘样本 {item['count']} 只",
        }
        for item in grouped[:6]
    ]
    industry_flows = [
        {
            "name": item["name"],
            "pct": round(item["pct_sum"] / item["count"], 2) if item["count"] else 0,
            "leader": item["leader"],
            "leader_symbol": "",
            "net_inflow": None,
            "leader_pct": None,
        }
        for item in grouped[:6]
    ]
    return hot_stocks, hot_boards, industry_flows


def _build_premarket_market_snapshot(candidates: List[Dict[str, Any]], reference_date: str) -> pd.DataFrame:
    rows = [
        {
            "ts_code": row.get("symbol"),
            "name": row.get("name"),
            "close": row.get("price"),
            "pct_chg": row.get("pct"),
            "amount": row.get("amount"),
            "timestamp": _display_trade_date(reference_date),
        }
        for row in candidates
    ]
    if not rows:
        return pd.DataFrame(columns=["ts_code", "name", "close", "pct_chg", "amount", "timestamp"])
    return pd.DataFrame(rows)


def _build_premarket_context(reference_date: str, candidate_limit: int = 12) -> Dict[str, Any]:
    candidates = _build_premarket_candidates(reference_date, limit=candidate_limit)
    hot_stocks, hot_boards, industry_flows = _build_premarket_hot_lists(candidates)
    market_snapshot = _build_premarket_market_snapshot(candidates, reference_date)
    return {
        "reference_date": reference_date,
        "reference_date_display": _display_trade_date(reference_date),
        "market_snapshot": market_snapshot,
        "hot_stocks": hot_stocks,
        "hot_boards": hot_boards,
        "industry_flows": industry_flows,
        "candidates": candidates,
        "rejected_candidates": [],
    }


def _build_premarket_outlook(reference_date: str, candidates: List[Dict[str, Any]]) -> Dict[str, Any]:
    top_candidate_score = max([int(item.get("score", 0)) for item in candidates], default=0)
    themes: List[Dict[str, Any]] = []
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for row in candidates:
        key = str(row.get("board_name") or row.get("industry_name") or "昨日强势股")
        grouped.setdefault(key, []).append(row)

    for name, rows in list(grouped.items())[:6]:
        leader = rows[0].get("name") or "-"
        themes.append(
            {
                "name": name,
                "leader": leader,
                "reason": f"盘前复盘候选 {len(rows)} 只",
                "view": "优先观察该方向是否在竞价和开盘后继续获得承接。",
            }
        )

    plans = []
    for row in candidates[:8]:
        score = int(row.get("score", 0))
        if score >= 70 or float(row.get("pct") or 0) >= 9.5:
            script = "高开强更强"
            action = "优先看竞价强度和开盘 5-15 分钟承接，强者优先。"
        elif score >= 50:
            script = "分歧后转强"
            action = "观察回踩后能否重新站回均线并放量。"
        else:
            script = "观察为主"
            action = "先看所属方向是否延续，再决定是否提升优先级。"

        plans.append(
            {
                "name": row.get("name"),
                "symbol": row.get("symbol"),
                "score": score,
                "board_name": row.get("board_name") or "-",
                "industry_name": row.get("industry_name") or "-",
                "script": script,
                "action": action,
                "risk": "若竞价明显转弱、开盘冲高回落或主线掉队，需要降低预期。",
                "why": row.get("score_breakdown") or row.get("signals_text") or "-",
            }
        )

    return {
        "summary": [
            {"label": "页面时间", "value": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "tone": "neutral"},
            {"label": "预案基准", "value": _display_trade_date(reference_date), "tone": "neutral"},
            {"label": "候选样本", "value": len(candidates), "tone": "up"},
            {"label": "龙头最高分", "value": top_candidate_score or "-", "tone": "neutral"},
        ],
        "market_view": {
            "tone": "up" if candidates else "warning",
            "title": "盘前预案模式",
            "message": (
                f"当前早于 09:15，先基于 {_display_trade_date(reference_date)} 的强势股复盘生成预案，"
                "09:30 后自动切回实时推演。"
            ),
        },
        "market_score": top_candidate_score,
        "market_reasons": [
            f"基准交易日 {_display_trade_date(reference_date)}",
            f"盘前候选 {len(candidates)} 只",
            "优先观察昨日强势股是否延续",
            "开盘后再用实时强度确认龙头",
        ],
        "themes": themes,
        "plans": plans,
        "rules": [
            "9:15 前以昨日强势股和涨停复盘做预案，不直接代替实盘信号。",
            "9:25-9:30 重点看竞价强弱和核心股是否继续排前。",
            "9:30 后优先使用实时龙头战法页面，不再只看盘前名单。",
            "若昨日强势方向竞价明显掉队，则先降低预期、等待新的实时主线。",
        ],
    }


def _build_leader_strategy_backtest(backtest_date: str, limit: int = 10) -> Dict[str, Any]:
    result: Dict[str, Any] = {
        "date": backtest_date,
        "date_display": _display_trade_date(backtest_date),
        "rows": [],
        "summary": [],
        "insights": [],
        "top_pick": None,
        "error": None,
        "note": (
            "这是一个复盘回放榜单：先取当天最终涨停池，再回看 10:00 前的涨幅和成交强度。"
            "它适合验证龙头战法的盘中判断，不等同于严格无泄漏量化回测。"
        ),
    }

    try:
        zt_df = ak.stock_zt_pool_em(date=backtest_date)
    except Exception as exc:
        result["error"] = f"未能获取 {result['date_display']} 的涨停池数据：{exc}"
        return result

    if not isinstance(zt_df, pd.DataFrame) or zt_df.empty:
        result["error"] = f"{result['date_display']} 没有可用的涨停池数据。"
        return result

    work_df = zt_df.copy()
    work_df["代码"] = work_df["代码"].astype(str).str.zfill(6)
    work_df["首次封板时间_num"] = pd.to_numeric(work_df["首次封板时间"], errors="coerce")
    late_df = work_df[work_df["首次封板时间_num"] > 100000].copy()
    if late_df.empty:
        result["error"] = f"{result['date_display']} 在 10:00 后没有新增涨停样本。"
        return result

    api = _build_local_zzshare_api()
    if api is None:
        result["error"] = "历史分钟线回放需要 `ZZSHARE_TOKEN`。请先配置 token 后再使用回测功能。"
        return result

    start_dt = f"{backtest_date} 09:30:00"
    end_dt = f"{backtest_date} 10:00:00"
    start_marker = f"{backtest_date}0930"
    end_marker = f"{backtest_date}1000"

    def analyze_row(row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        code = str(row.get("代码", "")).zfill(6)
        ts_code = _candidate_ts_codes(code)[0]
        try:
            minute_df = api.stk_mins(
                ts_code=ts_code,
                freq="1min",
                start_time=start_dt,
                end_time=end_dt,
            )
        except Exception:
            return None

        if not isinstance(minute_df, pd.DataFrame) or minute_df.empty:
            return None

        minute_df = minute_df.copy()
        minute_df["trade_time"] = minute_df["trade_time"].astype(str)
        minute_df = minute_df[(minute_df["trade_time"] >= start_marker) & (minute_df["trade_time"] <= end_marker)]
        if minute_df.empty:
            return None
        minute_df = minute_df.sort_values(by="trade_time", ascending=True).reset_index(drop=True)
        minute_df["high"] = pd.to_numeric(minute_df["high"], errors="coerce")
        minute_df["close"] = pd.to_numeric(minute_df["close"], errors="coerce")
        minute_df["amount"] = pd.to_numeric(minute_df["amount"], errors="coerce")

        final_price = float(row.get("最新价") or 0)
        final_pct = float(row.get("涨跌幅") or 0)
        if final_price <= 0:
            return None
        prev_close = final_price / (1 + final_pct / 100)

        last_row = minute_df.iloc[-1]
        price_1000 = float(last_row["close"])
        high_1000 = float(minute_df["high"].max())
        amount_1000 = float(minute_df["amount"].sum())
        pct_1000 = (price_1000 / prev_close - 1) * 100
        high_pct_1000 = (high_1000 / prev_close - 1) * 100

        score = 0
        signals: List[str] = []
        if pct_1000 >= 17:
            score += 40
            signals.append("10点涨幅极强")
        elif pct_1000 >= 12:
            score += 30
            signals.append("10点涨幅强")
        elif pct_1000 >= 9:
            score += 22
            signals.append("10点接近板位")
        elif pct_1000 >= 7:
            score += 16
            signals.append("10点保持强势")
        elif pct_1000 >= 5:
            score += 10
            signals.append("10点仍有明显强度")

        if high_pct_1000 >= 18:
            score += 16
            signals.append("盘中冲击涨停")
        elif high_pct_1000 >= 12:
            score += 10
            signals.append("盘中冲高明显")
        elif high_pct_1000 >= 8:
            score += 6
            signals.append("盘中持续拉升")

        if amount_1000 >= 2000000000:
            score += 18
            signals.append("10点前成交额超20亿")
        elif amount_1000 >= 1000000000:
            score += 12
            signals.append("10点前成交额超10亿")
        elif amount_1000 >= 500000000:
            score += 8
            signals.append("10点前成交额超5亿")
        elif amount_1000 >= 200000000:
            score += 4
            signals.append("10点前成交额超2亿")

        float_mkt_cap = float(row.get("流通市值") or 0)
        if 0 < float_mkt_cap <= 10000000000:
            score += 8
            signals.append("流通盘较小")
        elif float_mkt_cap <= 30000000000:
            score += 4
            signals.append("流通盘适中")

        return {
            "rank": 0,
            "symbol": ts_code,
            "code": code,
            "name": str(row.get("名称", "")).strip(),
            "industry": row.get("所属行业"),
            "price_1000": round(price_1000, 2),
            "pct_1000": round(pct_1000, 2),
            "high_pct_1000": round(high_pct_1000, 2),
            "amount_1000": amount_1000,
            "amount_1000_yi": round(amount_1000 / 100000000, 2),
            "score": score,
            "signals_text": "；".join(signals[:4]),
            "first_limit_time": str(row.get("首次封板时间", "")),
            "final_limit_price": round(final_price, 2),
            "final_day_pct": round(final_pct, 2),
            "status": "10点后涨停",
        }

    rows: List[Dict[str, Any]] = []
    failed_count = 0
    for row in late_df.to_dict(orient="records"):
        item = analyze_row(row)
        if item:
            rows.append(item)
        else:
            failed_count += 1

    rows.sort(
        key=lambda item: (
            item.get("score", 0),
            item.get("pct_1000", -999),
            item.get("amount_1000", -1),
            item.get("high_pct_1000", -999),
        ),
        reverse=True,
    )

    for index, row in enumerate(rows, start=1):
        row["rank"] = index

    result["rows"] = rows[:limit]
    result["top_pick"] = rows[0] if rows else None
    result["summary"] = [
        {"label": "回测日期", "value": result["date_display"], "tone": "neutral"},
        {"label": "涨停总样本", "value": int(len(work_df)), "tone": "neutral"},
        {"label": "10点后涨停样本", "value": int(len(late_df)), "tone": "neutral"},
        {"label": "可评估样本", "value": int(len(rows)), "tone": "neutral"},
    ]
    if rows:
        top3 = rows[: min(3, len(rows))]
        top10 = rows[: min(10, len(rows))]
        avg_top3_pct = sum(float(item.get("pct_1000", 0)) for item in top3) / len(top3)
        avg_top10_amount = sum(float(item.get("amount_1000_yi", 0)) for item in top10) / len(top10)
        earliest = min(rows, key=lambda item: str(item.get("first_limit_time", "999999")))
        result["insights"] = [
            {"label": "Top1 预测", "value": f"{rows[0]['name']} 命中", "tone": "up"},
            {"label": "Top3 平均强度", "value": f"{avg_top3_pct:.2f}%", "tone": "up"},
            {"label": "Top10 平均成交额", "value": f"{avg_top10_amount:.2f}亿", "tone": "neutral"},
            {"label": "最早首封", "value": _display_limit_time(earliest.get('first_limit_time')), "tone": "neutral"},
        ]
    if not rows and failed_count:
        result["error"] = (
            f"{result['date_display']} 的分钟线回放未取到有效样本，可能是东财分钟线源临时不可用。"
        )
    return result


def _collect_leader_strategy_pool(
    market_snapshot: pd.DataFrame,
    hot_stocks: List[Dict[str, Any]],
    industry_flows: List[Dict[str, Any]],
    hot_boards: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    snapshot_rows = market_snapshot.to_dict(orient="records") if isinstance(market_snapshot, pd.DataFrame) else []
    snapshot_by_name = {
        str(row.get("name")).strip(): row
        for row in snapshot_rows
        if str(row.get("name", "")).strip()
    }
    snapshot_by_symbol = {
        str(row.get("ts_code")).strip().upper(): row
        for row in snapshot_rows
        if str(row.get("ts_code", "")).strip()
    }
    hot_stock_rank = {
        str(row.get("symbol", "")).strip().upper(): int(row.get("rank", 999))
        for row in hot_stocks
        if str(row.get("symbol", "")).strip()
    }

    candidates: Dict[str, Dict[str, Any]] = {}

    def ensure_candidate(symbol: str, name: str) -> Optional[Dict[str, Any]]:
        normalized_symbol = str(symbol or "").strip().upper()
        normalized_name = str(name or "").strip()
        if not normalized_symbol and normalized_name:
            snapshot = snapshot_by_name.get(normalized_name)
            if snapshot:
                normalized_symbol = str(snapshot.get("ts_code", "")).strip().upper()
        if not normalized_name and normalized_symbol:
            snapshot = snapshot_by_symbol.get(normalized_symbol)
            if snapshot:
                normalized_name = str(snapshot.get("name", "")).strip()
        if not normalized_symbol and not normalized_name:
            return None
        key = normalized_symbol or normalized_name
        candidate = candidates.get(key)
        if candidate is None:
            snapshot = snapshot_by_symbol.get(normalized_symbol) or snapshot_by_name.get(normalized_name) or {}
            candidate = {
                "symbol": normalized_symbol or str(snapshot.get("ts_code", "")),
                "name": normalized_name or str(snapshot.get("name", "")),
                "price": snapshot.get("close"),
                "pct": snapshot.get("pct_chg"),
                "amount": snapshot.get("amount"),
                "score": 0,
                "signals": [],
                "score_items": [],
                "board_name": "",
                "industry_name": "",
                "hot_rank": None,
                "snapshot_rank": None,
                "has_board": False,
                "has_industry": False,
                "has_heat": False,
                "has_snapshot": False,
                "rejected_reasons": [],
            }
            candidates[key] = candidate
        return candidate

    strong_snapshot_rows = sorted(
        snapshot_rows,
        key=lambda row: (
            float(row.get("pct_chg") or -999),
            float(row.get("amount") or -1),
        ),
        reverse=True,
    )
    for index, row in enumerate(strong_snapshot_rows, start=1):
        symbol = str(row.get("ts_code", "")).strip()
        name = str(row.get("name", "")).strip()
        pct = row.get("pct_chg")
        amount = row.get("amount")
        if not symbol or not name:
            continue
        if not isinstance(pct, (int, float)) or not isinstance(amount, (int, float)):
            continue
        if pct < 5 and amount < 300000000:
            continue

        candidate = ensure_candidate(symbol, name)
        if not candidate:
            continue

        candidate["has_snapshot"] = True
        candidate["snapshot_rank"] = index
        candidate["price"] = row.get("close")
        candidate["pct"] = pct
        candidate["amount"] = amount

        if pct >= 9.7:
            snapshot_score = 18 if index <= 20 else 14
            candidate["score"] += snapshot_score
            candidate["signals"].append(f"市场强势: 盘中涨停/封板 (+{snapshot_score})")
            candidate["score_items"].append({"label": f"市场强势第 {index} 名，盘中涨停", "score": snapshot_score})
        elif pct >= 7:
            snapshot_score = 12 if index <= 20 else 8
            candidate["score"] += snapshot_score
            candidate["signals"].append(f"市场强势: 涨幅居前 (+{snapshot_score})")
            candidate["score_items"].append({"label": f"市场强势第 {index} 名，涨幅居前", "score": snapshot_score})
        elif amount >= 1000000000:
            snapshot_score = 6
            candidate["score"] += snapshot_score
            candidate["signals"].append(f"市场强势: 成交额居前 (+{snapshot_score})")
            candidate["score_items"].append({"label": f"市场成交活跃，排名第 {index} 名", "score": snapshot_score})

    for index, board in enumerate(hot_boards, start=1):
        candidate = ensure_candidate(str(board.get("leader_symbol", "")).strip(), str(board.get("leader", "")).strip())
        if not candidate:
            continue
        board_score = 38 if index <= 3 else 28 if index <= 6 else 18
        candidate["score"] += board_score
        candidate["board_name"] = candidate["board_name"] or str(board.get("name", ""))
        candidate["has_board"] = True
        candidate["signals"].append(f"题材龙头: {board.get('name')} (+{board_score})")
        candidate["score_items"].append({"label": f"题材龙头 {board.get('name')}", "score": board_score})

    for index, industry in enumerate(industry_flows, start=1):
        candidate = ensure_candidate(str(industry.get("leader_symbol", "")).strip(), str(industry.get("leader", "")).strip())
        if not candidate:
            continue
        industry_score = 24 if index <= 3 else 16 if index <= 6 else 10
        candidate["score"] += industry_score
        candidate["industry_name"] = candidate["industry_name"] or str(industry.get("name", ""))
        candidate["has_industry"] = True
        candidate["signals"].append(f"行业领涨: {industry.get('name')} (+{industry_score})")
        candidate["score_items"].append({"label": f"行业领涨 {industry.get('name')}", "score": industry_score})

    for row in hot_stocks:
        candidate = ensure_candidate(str(row.get("symbol", "")).strip(), str(row.get("name", "")).strip())
        if not candidate:
            continue
        rank = int(row.get("rank", 999))
        heat_score = 8 if rank <= 3 else 5 if rank <= 10 else 2
        candidate["score"] += heat_score
        candidate["hot_rank"] = rank
        candidate["has_heat"] = True
        candidate["signals"].append(f"热度靠前: 东财人气第 {rank} 名 (+{heat_score})")
        candidate["score_items"].append({"label": f"市场关注股第 {rank} 名", "score": heat_score})

    for candidate in candidates.values():
        pct = candidate.get("pct")
        amount = candidate.get("amount")
        name = str(candidate.get("name", ""))
        if isinstance(pct, (int, float)):
            if pct >= 9.5:
                candidate["score"] += 25
                candidate["signals"].append("盘中强度: 接近涨停 (+25)")
                candidate["score_items"].append({"label": f"盘中涨幅 {pct:.2f}% 接近涨停", "score": 25})
            elif pct >= 7:
                candidate["score"] += 20
                candidate["signals"].append("盘中强度: 涨幅强势 (+20)")
                candidate["score_items"].append({"label": f"盘中涨幅 {pct:.2f}% 强势", "score": 20})
            elif pct >= 5:
                candidate["score"] += 14
                candidate["signals"].append("盘中强度: 涨幅领先 (+14)")
                candidate["score_items"].append({"label": f"盘中涨幅 {pct:.2f}% 领先", "score": 14})
            elif pct >= 3:
                candidate["score"] += 8
                candidate["signals"].append("盘中强度: 有明显拉升 (+8)")
                candidate["score_items"].append({"label": f"盘中涨幅 {pct:.2f}% 有拉升", "score": 8})
            elif pct < 0:
                candidate["score"] -= 8
                candidate["signals"].append("盘中强度: 当前走弱 (-8)")
                candidate["score_items"].append({"label": f"盘中涨幅 {pct:.2f}% 走弱", "score": -8})
        if isinstance(amount, (int, float)):
            if amount >= 1500000000:
                candidate["score"] += 14
                candidate["signals"].append("成交额放大: 超 15 亿 (+14)")
                candidate["score_items"].append({"label": f"成交额 {amount / 100000000:.2f}亿 放大", "score": 14})
            elif amount >= 800000000:
                candidate["score"] += 10
                candidate["signals"].append("成交额活跃: 超 8 亿 (+10)")
                candidate["score_items"].append({"label": f"成交额 {amount / 100000000:.2f}亿 活跃", "score": 10})
            elif amount >= 300000000:
                candidate["score"] += 6
                candidate["signals"].append("成交额尚可: 超 3 亿 (+6)")
                candidate["score_items"].append({"label": f"成交额 {amount / 100000000:.2f}亿 尚可", "score": 6})
        if "ST" in name.upper():
            candidate["score"] -= 40
            candidate["signals"].append("风险过滤: ST 个股 (-40)")
            candidate["score_items"].append({"label": "ST 风险过滤", "score": -40})

    collected = [candidate for candidate in candidates.values() if candidate.get("name")]
    return collected


def _apply_leader_strategy_filters(candidates: List[Dict[str, Any]]) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    prefiltered = [candidate for candidate in candidates if candidate.get("score", 0) > 0]
    strict_filtered: List[Dict[str, Any]] = []
    rejected: List[Dict[str, Any]] = []
    for candidate in prefiltered:
        pct = candidate.get("pct")
        amount = candidate.get("amount")
        has_board = bool(candidate.get("has_board"))
        has_industry = bool(candidate.get("has_industry"))
        has_heat = bool(candidate.get("has_heat"))
        has_snapshot = bool(candidate.get("has_snapshot"))
        strong_pct = isinstance(pct, (int, float)) and pct >= 5
        active_amount = isinstance(amount, (int, float)) and amount >= 300000000
        limit_up_breakout = isinstance(pct, (int, float)) and pct >= 9.7 and active_amount
        theme_resonance = has_board or has_industry or limit_up_breakout
        resonance_count = sum([has_board, has_industry, has_heat, has_snapshot, strong_pct, active_amount])

        rejected_reasons: List[str] = []
        if not theme_resonance:
            rejected_reasons.append("没有题材/行业共振，且未达到强势涨停直通条件")
        if not strong_pct:
            rejected_reasons.append("盘中涨幅不足 5%")
        if resonance_count < 3:
            rejected_reasons.append(f"共振项不足，仅 {resonance_count} 个")

        candidate["resonance_count"] = resonance_count
        candidate["score_tags"] = [
            {
                "label": item["label"],
                "score": item["score"],
                "tone": "negative" if item["score"] < 0 else "positive",
            }
            for item in candidate["score_items"]
        ]
        candidate["entry_reason"] = (
            f"入池原因："
            f"{'有题材/行业共振' if (has_board or has_industry) else '市场强势直通' if limit_up_breakout else '无主线共振'}，"
            f"{'盘中强势' if strong_pct else '盘中强度不足'}，"
            f"共振项 {resonance_count} 个。"
        )
        candidate["score_breakdown"] = "；".join(
            f"{item['label']} ({item['score']:+d})" for item in candidate["score_items"]
        )
        if rejected_reasons:
            candidate["rejected_reasons"] = rejected_reasons
            candidate["rejected_reason_text"] = "；".join(rejected_reasons)
            rejected.append(candidate)
            continue

        strict_filtered.append(candidate)

    strict_filtered.sort(
        key=lambda item: (
            item.get("score", 0),
            item.get("resonance_count", 0),
            item.get("pct") if isinstance(item.get("pct"), (int, float)) else -999,
            item.get("amount") if isinstance(item.get("amount"), (int, float)) else -1,
        ),
        reverse=True,
    )

    rejected.sort(
        key=lambda item: (
            item.get("score", 0),
            item.get("pct") if isinstance(item.get("pct"), (int, float)) else -999,
            item.get("amount") if isinstance(item.get("amount"), (int, float)) else -1,
        ),
        reverse=True,
    )

    for index, candidate in enumerate(strict_filtered, start=1):
        candidate["rank"] = index
        candidate["signals_text"] = "；".join(candidate["signals"][:4])

    return strict_filtered, rejected


def _build_leader_strategy_candidates(
    market_snapshot: pd.DataFrame,
    hot_stocks: List[Dict[str, Any]],
    industry_flows: List[Dict[str, Any]],
    hot_boards: List[Dict[str, Any]],
    limit: int = 12,
) -> List[Dict[str, Any]]:
    pool = _collect_leader_strategy_pool(
        market_snapshot=market_snapshot,
        hot_stocks=hot_stocks,
        industry_flows=industry_flows,
        hot_boards=hot_boards,
    )
    accepted, _ = _apply_leader_strategy_filters(pool)
    return accepted[:limit]


def _build_leader_strategy_rejected_candidates(
    market_snapshot: pd.DataFrame,
    hot_stocks: List[Dict[str, Any]],
    industry_flows: List[Dict[str, Any]],
    hot_boards: List[Dict[str, Any]],
    limit: int = 12,
) -> List[Dict[str, Any]]:
    pool = _collect_leader_strategy_pool(
        market_snapshot=market_snapshot,
        hot_stocks=hot_stocks,
        industry_flows=industry_flows,
        hot_boards=hot_boards,
    )
    _, rejected = _apply_leader_strategy_filters(pool)
    return rejected[:limit]


def _build_tomorrow_outlook(
    market_snapshot: pd.DataFrame,
    hot_stocks: List[Dict[str, Any]],
    industry_flows: List[Dict[str, Any]],
    hot_boards: List[Dict[str, Any]],
    candidates: List[Dict[str, Any]],
) -> Dict[str, Any]:
    pct_series = pd.to_numeric(market_snapshot.get("pct_chg"), errors="coerce").fillna(0) if not market_snapshot.empty else pd.Series(dtype=float)
    avg_pct = float(pct_series.mean()) if not pct_series.empty else 0.0
    strong_count = int((pct_series >= 7).sum()) if not pct_series.empty else 0
    weak_count = int((pct_series <= -3).sum()) if not pct_series.empty else 0
    top_board_count = len(hot_boards[:3])
    top_industry_count = len(industry_flows[:3])
    top_candidate_score = max([int(item.get("score", 0)) for item in candidates], default=0)

    market_score = 0
    reasons: List[str] = []
    if avg_pct >= 4:
        market_score += 10
        reasons.append(f"市场样本平均涨幅 {avg_pct:.2f}%")
    elif avg_pct >= 2:
        market_score += 6
        reasons.append(f"市场样本平均涨幅 {avg_pct:.2f}%")
    elif avg_pct < 0:
        market_score -= 6
        reasons.append(f"市场样本平均涨幅 {avg_pct:.2f}% 偏弱")

    if strong_count >= 12:
        market_score += 8
        reasons.append(f"强势样本 {strong_count} 只")
    elif strong_count >= 6:
        market_score += 4
        reasons.append(f"强势样本 {strong_count} 只")

    if weak_count >= 8:
        market_score -= 5
        reasons.append(f"走弱样本 {weak_count} 只")

    if top_board_count >= 3:
        market_score += 5
        reasons.append("热门题材集中度较高")
    if top_industry_count >= 3:
        market_score += 4
        reasons.append("行业强度榜前列清晰")
    if top_candidate_score >= 70:
        market_score += 6
        reasons.append(f"龙头候选最高分 {top_candidate_score}")
    elif top_candidate_score >= 55:
        market_score += 3
        reasons.append(f"龙头候选最高分 {top_candidate_score}")

    if market_score >= 18:
        market_view = {
            "tone": "up",
            "title": "明日偏强延续",
            "message": "主线题材和龙头股延续概率较高，更像强势市场中的分歧转一致。",
        }
    elif market_score >= 10:
        market_view = {
            "tone": "up",
            "title": "明日震荡偏强",
            "message": "主线仍在，但更可能出现高开分化与盘中换手，适合盯紧核心票。",
        }
    elif market_score >= 3:
        market_view = {
            "tone": "neutral",
            "title": "明日偏分化",
            "message": "板块仍有活跃点，但不是普涨环境，更适合只看龙一和龙二。",
        }
    else:
        market_view = {
            "tone": "warning",
            "title": "明日谨慎防守",
            "message": "强度不够集中，容易出现冲高回落，追涨风险较高。",
        }

    themes = []
    for index, board in enumerate(hot_boards[:6], start=1):
        leader = board.get("leader") or "-"
        if index <= 2:
            view = "有望继续作为主线观察对象"
        elif index <= 4:
            view = "更像次主线，需看明天是否有资金回流"
        else:
            view = "偏轮动题材，适合观察不宜重仓预判"
        themes.append(
            {
                "name": board.get("name"),
                "leader": leader,
                "reason": board.get("reason"),
                "view": view,
            }
        )

    plans = []
    for row in candidates[:8]:
        pct = row.get("pct") if isinstance(row.get("pct"), (int, float)) else 0.0
        score = int(row.get("score", 0))
        if score >= 75 or pct >= 9.5:
            script = "高开强更强"
            action = "关注高开后 5-15 分钟是否继续放量上攻，若快速转弱则不追。"
        elif score >= 60 or pct >= 7:
            script = "分歧后转强"
            action = "重点看低开或平开后的承接，若回踩后重新放量站回均线可继续跟踪。"
        elif score >= 45:
            script = "冲高确认"
            action = "更适合作为备选，只有板块同步走强时才有提升为核心的机会。"
        else:
            script = "观察为主"
            action = "暂时不作为第一梯队，只看是否被主线情绪带动。"
        risk = "若题材掉队、成交缩量或开盘 30 分钟内跌破昨日强势区，容易从龙头候选降级为跟风。"
        plans.append(
            {
                "name": row.get("name"),
                "symbol": row.get("symbol"),
                "score": score,
                "board_name": row.get("board_name") or "-",
                "industry_name": row.get("industry_name") or "-",
                "script": script,
                "action": action,
                "risk": risk,
                "why": row.get("score_breakdown") or row.get("signals_text") or "-",
            }
        )

    tomorrow_rules = [
        "优先只盯前 3 名题材和前 3 名候选股，不要把精力分散到后排。",
        "若主线题材高开后继续放量，优先看龙一的承接，不追已经明显掉队的票。",
        "若高开分歧但核心票不破关键均线，可等二次转强；若题材整体转弱，则先观望。",
        "热度榜只做辅助确认，不单独作为买入理由。",
    ]

    summary = [
        {"label": "页面时间", "value": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "tone": "neutral"},
        {"label": "平均涨幅", "value": f"{avg_pct:.2f}%", "tone": "up" if avg_pct >= 0 else "down"},
        {"label": "强势样本", "value": strong_count, "tone": "up"},
        {"label": "龙头最高分", "value": top_candidate_score or "-", "tone": "neutral"},
    ]

    return {
        "summary": summary,
        "market_view": market_view,
        "market_score": market_score,
        "market_reasons": reasons,
        "themes": themes,
        "plans": plans,
        "rules": tomorrow_rules,
    }


def _safe_daily_snapshot(api: DataApi, trade_date: str, limit: int = 18) -> pd.DataFrame:
    df = _safe_call(
        api.daily,
        trade_date=trade_date,
        limit=limit,
        fields="ts_code,trade_date,open,high,low,close,pct_chg,vol,amount",
    )
    if isinstance(df, pd.DataFrame):
        return df
    return pd.DataFrame(
        columns=["ts_code", "trade_date", "open", "high", "low", "close", "pct_chg", "vol", "amount"]
    )


def _build_market_summary(df: pd.DataFrame, trade_date: str) -> List[Dict[str, Any]]:
    if df.empty:
        return [
            {"label": "交易日", "value": trade_date, "tone": "neutral"},
            {"label": "样本数量", "value": 0, "tone": "neutral"},
            {"label": "平均涨跌幅", "value": "-", "tone": "neutral"},
            {"label": "涨停样本", "value": 0, "tone": "up"},
        ]
    pct = pd.to_numeric(df.get("pct_chg"), errors="coerce").fillna(0)
    up_limit_count = int((pct >= 9.8).sum())
    down_limit_count = int((pct <= -9.8).sum())
    avg_pct = pct.mean()
    return [
        {"label": "交易日", "value": trade_date, "tone": "neutral"},
        {"label": "样本数量", "value": len(df), "tone": "neutral"},
        {"label": "平均涨跌幅", "value": f"{avg_pct:.2f}%", "tone": "up" if avg_pct >= 0 else "down"},
        {"label": "涨停样本", "value": up_limit_count, "tone": "up"},
        {"label": "跌停样本", "value": down_limit_count, "tone": "down"},
    ]


def _top_movers(df: pd.DataFrame, limit: int = 8) -> List[Dict[str, Any]]:
    if df.empty:
        return []
    view = df.copy()
    view["pct_chg"] = pd.to_numeric(view.get("pct_chg"), errors="coerce")
    view["amount"] = pd.to_numeric(view.get("amount"), errors="coerce")
    view = view.sort_values(by=["pct_chg", "amount"], ascending=[False, False]).head(limit)
    return view.to_dict(orient="records")


def _normalize_ths_hot(data: Any) -> List[Dict[str, Any]]:
    if not isinstance(data, list):
        return []
    rows: List[Dict[str, Any]] = []
    for item in data:
        if not isinstance(item, dict):
            continue
        symbol = item.get("symbol_code")
        rows.append(
            {
                "rank": item.get("rank"),
                "symbol": f"{symbol}.SH" if str(symbol).startswith("6") else f"{symbol}.SZ" if str(symbol).startswith(("0", "3")) else symbol,
                "name": item.get("symbol_name"),
                "price": item.get("last_price"),
                "pct": item.get("last_pct"),
                "rank_diff": item.get("rank_diff"),
            }
        )
    return rows


def _normalize_lhb(data: Any, limit: int = 10) -> List[Dict[str, Any]]:
    if not isinstance(data, list):
        return []
    rows: List[Dict[str, Any]] = []
    for item in data[:limit]:
        if not isinstance(item, dict):
            continue
        rows.append(
            {
                "symbol": item.get("stock_code"),
                "name": item.get("stock_name"),
                "pct": item.get("quote_change"),
                "turnover": item.get("turnover"),
                "reason": item.get("up_reason"),
                "tag": item.get("up_desc"),
            }
        )
    return rows


def _normalize_hot_boards(data: Any, limit: int = 8) -> List[Dict[str, Any]]:
    if isinstance(data, dict):
        structured = _normalize_hot_boards_from_plate_payload(data, limit=limit)
        if structured:
            return structured
        aggregated = _aggregate_hot_boards_from_stock_map(data, limit=limit)
        if aggregated:
            return aggregated
    candidates = _extract_candidates(data)
    rows: List[Dict[str, Any]] = []
    for item in candidates:
        if not isinstance(item, dict):
            continue
        name = item.get("name") or item.get("plate_name") or item.get("block_name") or item.get("title")
        if not name:
            continue
        rows.append(
            {
                "name": name,
                "reason": item.get("reason") or item.get("up_reason") or item.get("summary") or item.get("desc") or "",
                "count": item.get("limit_up_num") or item.get("num") or item.get("count") or item.get("join_num") or 0,
                "leader": item.get("dragon") or item.get("leader") or item.get("stock_name") or "",
            }
        )
        if len(rows) >= limit:
            break
    return rows


def _normalize_hot_boards_from_plate_payload(data: Dict[str, Any], limit: int = 8) -> List[Dict[str, Any]]:
    plate_rows = data.get("plate")
    plate_info = data.get("plate_info")
    plate_stocks = data.get("plate_stocks")
    if not isinstance(plate_rows, list):
        return []

    rows: List[Dict[str, Any]] = []
    for item in plate_rows[:limit]:
        if not isinstance(item, list) or len(item) < 2:
            continue
        plate_name = str(item[0]).strip()
        plate_code = str(item[1]).strip()
        score = item[2] if len(item) > 2 else 0
        if not plate_name or not plate_code:
            continue

        stock_items = plate_stocks.get(plate_code, []) if isinstance(plate_stocks, dict) else []
        leader_name = ""
        if isinstance(stock_items, list) and stock_items:
            leader = max(
                (stock for stock in stock_items if isinstance(stock, dict)),
                key=lambda stock: float(stock.get("amount") or 0),
                default=None,
            )
            if isinstance(leader, dict):
                leader_name = leader.get("stock_name") or leader.get("stock_code") or ""

        count = len(stock_items) if isinstance(stock_items, list) else 0
        reason = f"热度分值 {score}"
        if isinstance(plate_info, dict) and isinstance(plate_info.get(plate_code), dict):
            info = plate_info[plate_code]
            info_score = info.get("score")
            if info_score not in (None, ""):
                reason = f"热度分值 {info_score}"

        rows.append(
            {
                "name": plate_name,
                "count": count,
                "leader": leader_name,
                "reason": reason,
            }
        )

    return rows


def _aggregate_hot_boards_from_stock_map(data: Dict[str, Any], limit: int = 8) -> List[Dict[str, Any]]:
    stock_scores = data.get("stocks_hot", {})
    plate_stats: Dict[str, Dict[str, Any]] = {}
    for stock_code, payload in data.items():
        if stock_code in {"stocks", "stocks_hot", "stocks_hot_n", "today"}:
            continue
        if not isinstance(payload, dict):
            continue
        plates = payload.get("plates")
        if not isinstance(plates, list):
            continue
        score = 0
        if isinstance(stock_scores, dict):
            try:
                score = int(stock_scores.get(stock_code, 0))
            except (TypeError, ValueError):
                score = 0
        for plate in plates:
            plate_name = str(plate).strip()
            if not plate_name:
                continue
            stat = plate_stats.setdefault(
                plate_name,
                {"name": plate_name, "count": 0, "score": 0, "leader": "", "leader_score": -1},
            )
            stat["count"] += 1
            stat["score"] += score
            if score > stat["leader_score"]:
                stat["leader_score"] = score
                stat["leader"] = stock_code

    rows = sorted(
        plate_stats.values(),
        key=lambda item: (item["count"], item["score"], item["name"]),
        reverse=True,
    )
    return [
        {
            "name": item["name"],
            "count": item["count"],
            "leader": item["leader"],
            "reason": f"关联 {item['count']} 只个股，累计热度 {item['score']}",
        }
        for item in rows[:limit]
    ]


def _extract_candidates(data: Any) -> List[Dict[str, Any]]:
    if isinstance(data, list):
        return [item for item in data if isinstance(item, dict)]
    if not isinstance(data, dict):
        return []
    preferred_keys = ["list", "items", "data", "rows", "plates", "hot_list", "sectors"]
    for key in preferred_keys:
        value = data.get(key)
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]
    flattened: List[Dict[str, Any]] = []
    for value in data.values():
        if isinstance(value, list):
            flattened.extend(item for item in value if isinstance(item, dict))
    return flattened


def _search_stocks(api: DataApi, query: str) -> List[Dict[str, Any]]:
    fields = "ts_code,symbol,name,market,exchange,list_status"
    normalized_query = query.strip().upper()
    code_like = any(ch.isdigit() for ch in normalized_query) or "." in normalized_query
    results = pd.DataFrame()
    if code_like:
        for ts_code in _candidate_ts_codes(normalized_query):
            code_result = _safe_call(api.stock_basic, ts_code=ts_code, fields=fields)
            if isinstance(code_result, pd.DataFrame) and not code_result.empty:
                results = code_result
                break
    if results.empty:
        name_result = _safe_call(api.stock_basic, name=query, fields=fields)
        if isinstance(name_result, pd.DataFrame):
            results = name_result
    if results.empty:
        return []
    return results.head(10).to_dict(orient="records")


def _candidate_ts_codes(query: str) -> List[str]:
    normalized = query.strip().upper()
    if "." in normalized:
        return [normalized]
    if normalized.isdigit() and len(normalized) == 6:
        if normalized.startswith("6"):
            return [f"{normalized}.SH"]
        if normalized.startswith(("0", "3")):
            return [f"{normalized}.SZ"]
        if normalized.startswith(("4", "8")):
            return [f"{normalized}.BJ"]
    return [normalized]


def _choose_stock(results: List[Dict[str, Any]], selected_symbol: str) -> Optional[Dict[str, Any]]:
    if not results:
        return None
    if not selected_symbol:
        return results[0]
    normalized = selected_symbol.strip().upper()
    for item in results:
        if str(item.get("ts_code", "")).upper() == normalized or str(item.get("symbol", "")).upper() == normalized:
            return item
    return results[0]


def _daily_chart(api: DataApi, ts_code: str) -> List[Dict[str, Any]]:
    df = _safe_call(
        api.daily,
        ts_code=ts_code,
        limit=60,
        fields="ts_code,trade_date,open,high,low,close,pct_chg,vol,amount",
    )
    if not isinstance(df, pd.DataFrame) or df.empty:
        return []
    chart = df.sort_values(by="trade_date", ascending=True).copy()
    chart["trade_date"] = chart["trade_date"].astype(str)
    return chart.to_dict(orient="records")


def _intraday_chart(ts_code: str) -> List[Dict[str, Any]]:
    api = _build_local_zzshare_api()
    if api is None:
        return []

    trade_date = _get_latest_trade_date(api)
    df = _safe_call(
        api.stk_mins,
        ts_code=ts_code,
        freq="1min",
        start_time=f"{trade_date} 09:30:00",
        end_time=f"{trade_date} 15:00:00",
    )
    if not isinstance(df, pd.DataFrame) or df.empty:
        return []

    intraday = df.copy()
    intraday["trade_time"] = intraday["trade_time"].astype(str)
    intraday = intraday.sort_values(by="trade_time", ascending=True).reset_index(drop=True)
    intraday["close"] = pd.to_numeric(intraday["close"], errors="coerce")
    intraday["vol"] = pd.to_numeric(intraday["vol"], errors="coerce")
    intraday["amount"] = pd.to_numeric(intraday["amount"], errors="coerce")
    if "avg_price" in intraday.columns:
        intraday["avg_price"] = pd.to_numeric(intraday["avg_price"], errors="coerce")
    else:
        intraday["avg_price"] = intraday["close"]
        valid_vol = intraday["vol"].fillna(0) > 0
        intraday.loc[valid_vol, "avg_price"] = intraday.loc[valid_vol, "amount"] / intraday.loc[valid_vol, "vol"]
    return intraday[["trade_time", "close", "avg_price"]].to_dict(orient="records")


def _latest_quote(api: DataApi, ts_code: str) -> Optional[Dict[str, Any]]:
    quote_map = load_sina_quotes([ts_code])
    return quote_map.get(ts_code)


def _generate_stock_advice(
    stock: Dict[str, Any],
    chart: List[Dict[str, Any]],
    latest_quote: Optional[Dict[str, Any]],
) -> tuple[Optional[str], Optional[str]]:
    if not is_doubao_configured():
        return None, (
            "尚未配置豆包模型。请先设置环境变量 DOUBAO_API_KEY 和 DOUBAO_MODEL，"
            "然后重启门户。"
        )

    try:
        client = DoubaoClient()
        prompt = _build_stock_advice_prompt(stock, chart, latest_quote)
        advice = client.chat(
            messages=[
                {
                    "role": "system",
                    "content": (
                        "你是一名谨慎的 A 股研究助手。请基于给定数据输出中文分析，"
                        "不要编造不存在的数据，不要承诺收益，明确区分机会与风险。"
                    ),
                },
                {"role": "user", "content": prompt},
            ],
            temperature=0.3,
            max_tokens=1200,
        )
        return advice, None
    except DoubaoConfigError as exc:
        return None, str(exc)
    except DoubaoRequestError as exc:
        return None, str(exc)
    except Exception as exc:
        return None, f"豆包分析失败: {exc}"


def _build_stock_advice_prompt(
    stock: Dict[str, Any],
    chart: List[Dict[str, Any]],
    latest_quote: Optional[Dict[str, Any]],
) -> str:
    latest_rows = chart[-20:]
    chart_lines = []
    for row in latest_rows:
        chart_lines.append(
            f"{row.get('trade_date')} 收盘={row.get('close')} 涨跌幅={row.get('pct_chg')}% 成交额={row.get('amount')}"
        )

    quote_text = "无实时快照"
    if latest_quote:
        quote_text = (
            f"最新价={latest_quote.get('close')} 开盘={latest_quote.get('open')} "
            f"最高={latest_quote.get('high')} 最低={latest_quote.get('low')} "
            f"成交额={latest_quote.get('amount')}"
        )

    return (
        "请基于下面的 A 股个股数据，给出一份适合散户阅读的“选股建议/观察建议”。\n"
        "要求输出 4 个部分：\n"
        "1. 核心结论\n"
        "2. 支持看多的信号\n"
        "3. 主要风险点\n"
        "4. 明日观察位与操作建议\n\n"
        f"股票名称: {stock.get('name')}\n"
        f"股票代码: {stock.get('ts_code')}\n"
        f"交易所: {stock.get('exchange')}\n"
        f"板块: {stock.get('market') or '主板'}\n"
        f"实时快照: {quote_text}\n"
        "最近 20 个交易日数据:\n"
        + "\n".join(chart_lines)
    )


def main() -> None:
    app = create_app()
    app.run(host="127.0.0.1", port=8000, debug=False)


if __name__ == "__main__":
    main()
