"""
Aggregate Refresh DAG - Rebuild warehouse aggregate tables.
Phase 6: Data Pipeline Orchestration (Aggregation)

Schedule: Triggered after warehouse load completes
Depends on: warehouse_load_dag
"""

import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict
from urllib.parse import unquote, urlparse

import psycopg2
from airflow.sdk import DAG, task

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import settings  # noqa: E402

logger = logging.getLogger(__name__)


def _parse_postgres_connection_kwargs(db_url: str) -> Dict[str, Any]:
    """Parse DB URL and return psycopg2 connection kwargs."""
    parsed = urlparse(db_url)
    if not parsed.scheme.startswith("postgresql"):
        raise ValueError(f"Unsupported warehouse driver in DB URL: {parsed.scheme}")

    return {
        "host": parsed.hostname or "localhost",
        "port": parsed.port or 5432,
        "dbname": parsed.path.lstrip("/"),
        "user": unquote(parsed.username or ""),
        "password": unquote(parsed.password or ""),
    }


def _refresh_daily_summary(cursor) -> int:
    """Rebuild daily_price_summary from fact table."""
    cursor.execute("TRUNCATE TABLE daily_price_summary")
    cursor.execute(
        """
        INSERT INTO daily_price_summary (
            summary_id,
            date_key,
            commodity_key,
            market_key,
            open_price,
            high_price,
            low_price,
            close_price,
            volume,
            price_change,
            price_change_pct,
            source_count,
            record_count
        )
        WITH ranked AS (
            SELECT
                f.*,
                ROW_NUMBER() OVER (
                    PARTITION BY f.date_key, f.commodity_key, f.market_key
                    ORDER BY f.price_id ASC
                ) AS rn_open,
                ROW_NUMBER() OVER (
                    PARTITION BY f.date_key, f.commodity_key, f.market_key
                    ORDER BY f.price_id DESC
                ) AS rn_close
            FROM commodity_prices_fact f
        ),
        grouped AS (
            SELECT
                date_key,
                commodity_key,
                market_key,
                MAX(CASE WHEN rn_open = 1 THEN open_price END) AS open_price,
                MAX(high_price) AS high_price,
                MIN(low_price) AS low_price,
                MAX(CASE WHEN rn_close = 1 THEN close_price END) AS close_price,
                SUM(COALESCE(volume, 0)) AS volume,
                COUNT(DISTINCT source_key) AS source_count,
                COUNT(*) AS record_count
            FROM ranked
            GROUP BY date_key, commodity_key, market_key
        )
        SELECT
            ROW_NUMBER() OVER (ORDER BY date_key, commodity_key, market_key) AS summary_id,
            date_key,
            commodity_key,
            market_key,
            ROUND(open_price::numeric, 2) AS open_price,
            ROUND(high_price::numeric, 2) AS high_price,
            ROUND(low_price::numeric, 2) AS low_price,
            ROUND(close_price::numeric, 2) AS close_price,
            ROUND(volume::numeric, 2) AS volume,
            ROUND((close_price - open_price)::numeric, 2) AS price_change,
            CASE
                WHEN open_price IS NOT NULL AND open_price <> 0
                    THEN ROUND((((close_price - open_price) / open_price) * 100)::numeric, 2)
                ELSE NULL
            END AS price_change_pct,
            source_count,
            record_count
        FROM grouped
        """
    )
    cursor.execute("SELECT COUNT(*) FROM daily_price_summary")
    return int(cursor.fetchone()[0])


def _refresh_weekly_summary(cursor) -> int:
    """Rebuild weekly_commodity_summary from fact + dim_date."""
    cursor.execute("TRUNCATE TABLE weekly_commodity_summary")
    cursor.execute(
        """
        INSERT INTO weekly_commodity_summary (
            summary_id,
            year,
            week_of_year,
            commodity_key,
            avg_price,
            min_price,
            max_price,
            total_volume
        )
        WITH grouped AS (
            SELECT
                d.year,
                d.week_of_year,
                f.commodity_key,
                AVG(f.close_price) AS avg_price,
                MIN(f.low_price) AS min_price,
                MAX(f.high_price) AS max_price,
                SUM(COALESCE(f.volume, 0)) AS total_volume
            FROM commodity_prices_fact f
            JOIN dim_date d ON d.date_key = f.date_key
            GROUP BY d.year, d.week_of_year, f.commodity_key
        )
        SELECT
            ROW_NUMBER() OVER (ORDER BY year, week_of_year, commodity_key) AS summary_id,
            year,
            week_of_year,
            commodity_key,
            ROUND(avg_price::numeric, 2) AS avg_price,
            ROUND(min_price::numeric, 2) AS min_price,
            ROUND(max_price::numeric, 2) AS max_price,
            ROUND(total_volume::numeric, 2) AS total_volume
        FROM grouped
        """
    )
    cursor.execute("SELECT COUNT(*) FROM weekly_commodity_summary")
    return int(cursor.fetchone()[0])


def _refresh_monthly_summary(cursor) -> int:
    """Rebuild monthly_commodity_summary from fact + dim_date."""
    cursor.execute("TRUNCATE TABLE monthly_commodity_summary")
    cursor.execute(
        """
        INSERT INTO monthly_commodity_summary (
            summary_id,
            year,
            month,
            commodity_key,
            avg_price,
            min_price,
            max_price,
            total_volume,
            price_volatility
        )
        WITH grouped AS (
            SELECT
                d.year,
                d.month,
                f.commodity_key,
                AVG(f.close_price) AS avg_price,
                MIN(f.low_price) AS min_price,
                MAX(f.high_price) AS max_price,
                SUM(COALESCE(f.volume, 0)) AS total_volume,
                STDDEV_SAMP(f.close_price) AS price_volatility
            FROM commodity_prices_fact f
            JOIN dim_date d ON d.date_key = f.date_key
            GROUP BY d.year, d.month, f.commodity_key
        )
        SELECT
            ROW_NUMBER() OVER (ORDER BY year, month, commodity_key) AS summary_id,
            year,
            month,
            commodity_key,
            ROUND(avg_price::numeric, 2) AS avg_price,
            ROUND(min_price::numeric, 2) AS min_price,
            ROUND(max_price::numeric, 2) AS max_price,
            ROUND(total_volume::numeric, 2) AS total_volume,
            ROUND(price_volatility::numeric, 2) AS price_volatility
        FROM grouped
        """
    )
    cursor.execute("SELECT COUNT(*) FROM monthly_commodity_summary")
    return int(cursor.fetchone()[0])


def _count_fact_rows(cursor) -> int:
    cursor.execute("SELECT COUNT(*) FROM commodity_prices_fact")
    return int(cursor.fetchone()[0])


default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "email": ["civaschenko@yahoo.com"],
    "email_on_failure": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="aggregate_refresh_dag",
    default_args=default_args,
    description="Refresh warehouse aggregate tables from fact data",
    schedule=None,
    catchup=False,
    tags=["warehouse", "aggregation", "daily"],
) as dag:

    @task()
    def refresh_aggregate_tables() -> Dict[str, Any]:
        """Rebuild aggregate tables in one DB transaction for consistency."""
        conn_kwargs = _parse_postgres_connection_kwargs(settings.database_url)

        with psycopg2.connect(**conn_kwargs) as conn:
            with conn.cursor() as cursor:
                fact_rows = _count_fact_rows(cursor)
                daily_rows = _refresh_daily_summary(cursor)
                weekly_rows = _refresh_weekly_summary(cursor)
                monthly_rows = _refresh_monthly_summary(cursor)

        return {
            "status": "refreshed",
            "fact_rows": fact_rows,
            "daily_rows": daily_rows,
            "weekly_rows": weekly_rows,
            "monthly_rows": monthly_rows,
        }

    @task()
    def verify_aggregate_refresh(stats: Dict[str, Any]) -> Dict[str, Any]:
        """Validate that aggregate refresh produced expected table state."""
        fact_rows = int(stats.get("fact_rows", 0))
        daily_rows = int(stats.get("daily_rows", 0))
        weekly_rows = int(stats.get("weekly_rows", 0))
        monthly_rows = int(stats.get("monthly_rows", 0))

        checks = {
            "refresh_task_completed": stats.get("status") == "refreshed",
            "daily_non_negative": daily_rows >= 0,
            "weekly_non_negative": weekly_rows >= 0,
            "monthly_non_negative": monthly_rows >= 0,
            "aggregates_present_when_facts_exist": (
                fact_rows == 0 or (daily_rows > 0 and weekly_rows > 0 and monthly_rows > 0)
            ),
        }
        passed = sum(1 for ok in checks.values() if ok)

        return {
            "status": "verified" if passed == len(checks) else "warning",
            "checks_passed": passed,
            "checks_total": len(checks),
            "checks": checks,
            "stats": stats,
        }

    refresh_stats = refresh_aggregate_tables()
    verify_aggregate_refresh(refresh_stats)
