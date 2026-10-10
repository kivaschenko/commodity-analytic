"""
Telegram Report DAG - Post a brief daily analytic digest from the gold layer.

Schedule: weekdays 08:00 UTC (after the nightly pipeline has refreshed aggregates)
Reads:    daily_price_summary, weekly_commodity_summary, dim_commodity, dim_date
Env:      TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
"""

import html
import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List
from urllib.parse import unquote, urlparse

import psycopg2
import requests
from airflow.sdk import DAG, task

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import settings  # noqa: E402

logger = logging.getLogger(__name__)

TOP_N = 3
TELEGRAM_LIMIT = 4096

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def _connect():
    parsed = urlparse(settings.database_url)
    return psycopg2.connect(
        host=parsed.hostname or "localhost",
        port=parsed.port or 5432,
        dbname=parsed.path.lstrip("/"),
        user=unquote(parsed.username or ""),
        password=unquote(parsed.password or ""),
    )


def _fmt_row(r: Dict[str, Any]) -> str:
    arrow = "🟢" if r["change_pct"] >= 0 else "🔴"
    return (
        f"{arrow} <b>{html.escape(r['name'])}</b>: {r['close']:.2f} "
        f"{html.escape(r['unit'] or '')} ({r['change_pct']:+.2f}%)"
    )


with DAG(
    dag_id="telegram_report_dag",
    default_args=default_args,
    description="Send brief analytic report from gold layer to Telegram",
    schedule="0 8 * * 1-5",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["reporting", "telegram", "gold"],
) as dag:

    @task()
    def fetch_gold_metrics() -> Dict[str, Any]:
        """Latest-day movers and week-over-week averages from gold tables."""
        with _connect() as conn, conn.cursor() as cur:
            cur.execute("SELECT MAX(date_key) FROM daily_price_summary")
            latest_key = cur.fetchone()[0]
            if latest_key is None:
                return {"empty": True}

            cur.execute(
                """
                SELECT d.calendar_date, c.commodity_name, c.unit,
                       AVG(s.close_price)::float,
                       COALESCE(AVG(s.price_change_pct), 0)::float
                FROM daily_price_summary s
                JOIN dim_commodity c ON c.commodity_key = s.commodity_key
                JOIN dim_date d ON d.date_key = s.date_key
                WHERE s.date_key = %s
                GROUP BY d.calendar_date, c.commodity_name, c.unit
                """,
                (latest_key,),
            )
            rows: List[Dict[str, Any]] = [
                {
                    "date": r[0].isoformat(),
                    "name": r[1],
                    "unit": r[2],
                    "close": r[3],
                    "change_pct": r[4],
                }
                for r in cur.fetchall()
            ]

            cur.execute(
                """
                SELECT c.commodity_name, w.avg_price::float
                FROM weekly_commodity_summary w
                JOIN dim_commodity c ON c.commodity_key = w.commodity_key
                WHERE (w.year, w.week_of_year) = (
                    SELECT year, week_of_year FROM weekly_commodity_summary
                    ORDER BY year DESC, week_of_year DESC LIMIT 1
                )
                ORDER BY w.avg_price DESC LIMIT %s
                """,
                (TOP_N,),
            )
            weekly = [{"name": r[0], "avg": r[1]} for r in cur.fetchall()]

        return {
            "empty": not rows,
            "date": rows[0]["date"] if rows else None,
            "rows": rows,
            "weekly": weekly,
        }

    @task()
    def build_message(metrics: Dict[str, Any]) -> str:
        if metrics.get("empty"):
            return ""
        rows = sorted(metrics["rows"], key=lambda r: r["change_pct"], reverse=True)
        gainers = [r for r in rows if r["change_pct"] > 0][:TOP_N]
        losers = [r for r in reversed(rows) if r["change_pct"] < 0][:TOP_N]
        avg_move = sum(r["change_pct"] for r in rows) / len(rows)

        lines = [
            f"📊 <b>Commodity Daily Brief — {metrics['date']}</b>",
            f"Tracked: {len(rows)} | Avg move: {avg_move:+.2f}%",
        ]
        if gainers:
            lines += ["", "<b>Top gainers</b>"] + [_fmt_row(r) for r in gainers]
        if losers:
            lines += ["", "<b>Top losers</b>"] + [_fmt_row(r) for r in losers]
        if metrics["weekly"]:
            lines += ["", "<b>Weekly avg price (highest)</b>"] + [
                f"• {html.escape(w['name'])}: {w['avg']:.2f}" for w in metrics["weekly"]
            ]
        message ="\n".join(lines)[:TELEGRAM_LIMIT]
        print(message)
        return message

    @task()
    def send_to_telegram(message: str) -> str:
        if not message:
            logger.warning("No gold data available; skipping Telegram report")
            return "skipped"
        token = os.environ["TELEGRAM_TOKEN"]
        chat_id = os.environ["TELEGRAM_CHANNEL_ID"]
        resp = requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={
                "chat_id": chat_id,
                "text": message,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            },
            timeout=30,
        )
        resp.raise_for_status()
        return "sent"

    send_to_telegram(build_message(fetch_gold_metrics()))
