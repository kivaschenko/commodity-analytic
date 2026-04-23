"""
Warehouse Load DAG - Load transformed silver data into warehouse dimensions and fact table.
Phase 5: Data Pipeline Orchestration (Warehouse Loading)

Schedule: Triggered after transformation completes
Depends on: transformation_dag
"""

import logging
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from airflow.sdk import DAG, task

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import Environment, settings  # noqa: E402
from staging.staging_handler import StagingHandler  # noqa: E402
from warehouse.loader import WarehouseLoader  # noqa: E402

logger = logging.getLogger(__name__)

SOURCES = ["yfinance", "graintradecomua", "tripoli_land"]
SOURCE_METADATA = {
    "yfinance": {
        "source_name": "Yahoo Finance",
        "parser_type": "yfinance_parser",
        "data_type": "futures",
        "reliability_rating": 4,
        "update_frequency": "daily",
        "api_endpoint": "https://finance.yahoo.com",
    },
    "graintradecomua": {
        "source_name": "GrainTrade UA",
        "parser_type": "graintradecomua_parser",
        "data_type": "spot_market",
        "reliability_rating": 3,
        "update_frequency": "intraday",
        "api_endpoint": "https://graintrade.com.ua",
    },
    "tripoli_land": {
        "source_name": "Tripoli Land",
        "parser_type": "tripoli_land_parser",
        "data_type": "storage_rates",
        "reliability_rating": 3,
        "update_frequency": "daily",
        "api_endpoint": "https://tripoli.land",
    },
}

CURRENCY_METADATA = {
    "USD": ("US Dollar", "United States"),
    "UAH": ("Ukrainian Hryvnia", "Ukraine"),
    "EUR": ("Euro", "European Union"),
}


def _storage_type() -> str:
    return "hetzner" if settings.env == Environment.PROD else "minio"


def _parse_timestamp(value: Any) -> Optional[datetime]:
    if value is None:
        return None

    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)

    if isinstance(value, str):
        for fmt in (
            "%Y-%m-%dT%H:%M:%S.%f%z",
            "%Y-%m-%dT%H:%M:%S%z",
            "%Y-%m-%dT%H:%M:%S.%f",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%d %H:%M:%S",
            "%d.%m.%Y %H:%M",
            "%Y-%m-%d",
        ):
            try:
                parsed = datetime.strptime(value, fmt)
                return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
            except ValueError:
                continue

    return None


def _safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _normalize_text(value: Any, fallback: str) -> str:
    if isinstance(value, str) and value.strip():
        return value.strip()
    return fallback


def _canonical_source(source: str, record: Dict[str, Any]) -> str:
    source_name = record.get("source_name")
    if isinstance(source_name, str) and source_name.strip():
        return source_name.strip()
    return SOURCE_METADATA.get(source, {}).get("source_name", source)


def _extract_dim_date(records: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    rows: List[Dict[str, Any]] = []
    key_map: Dict[str, int] = {}

    for record in records:
        ts = (
            _parse_timestamp(record.get("source_timestamp"))
            or _parse_timestamp(record.get("processed_at"))
            or _parse_timestamp(record.get("extracted_at"))
            or datetime.now(timezone.utc)
        )
        date_obj = ts.date()
        date_str = date_obj.isoformat()
        if date_str in key_map:
            continue

        date_key = int(date_obj.strftime("%Y%m%d"))
        key_map[date_str] = date_key
        rows.append(
            {
                "date_key": date_key,
                "calendar_date": date_str,
                "year": date_obj.year,
                "quarter": (date_obj.month - 1) // 3 + 1,
                "month": date_obj.month,
                "day": date_obj.day,
                "week_of_year": date_obj.isocalendar()[1],
                "day_of_week": date_obj.strftime("%A"),
                "is_weekend": date_obj.weekday() >= 5,
                "is_holiday": False,
                "trading_status": "active",
            }
        )

    return rows, key_map


def _extract_dim_commodity(records: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    rows: List[Dict[str, Any]] = []
    key_map: Dict[str, int] = {}
    commodity_type_map = {
        "wheat": ("grain", "cereals"),
        "corn": ("grain", "cereals"),
        "barley": ("grain", "cereals"),
        "rye": ("grain", "cereals"),
        "oats": ("grain", "cereals"),
        "soybeans": ("oil_crop", "legumes"),
        "sunflower": ("oil_crop", "oil_seeds"),
    }

    for record in records:
        commodity_name = _normalize_text(record.get("commodity_name"), "Unknown")
        map_key = commodity_name.lower()
        if map_key in key_map:
            continue

        ctype, category = commodity_type_map.get(map_key, ("unknown", "unknown"))
        commodity_key = len(key_map) + 1
        key_map[map_key] = commodity_key
        rows.append(
            {
                "commodity_key": commodity_key,
                "commodity_name": commodity_name,
                "commodity_type": ctype,
                "category": category,
                "unit": _normalize_text(record.get("unit"), "metric_ton"),
                "grade": record.get("grade"),
                "origin_country": record.get("market_country"),
                "is_active": True,
            }
        )

    return rows, key_map


def _extract_dim_market(records: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    rows: List[Dict[str, Any]] = []
    key_map: Dict[str, int] = {}

    for record in records:
        market_name = _normalize_text(
            record.get("market_name") or record.get("market") or record.get("region"),
            "Unknown Market",
        )
        map_key = market_name.lower()
        if map_key in key_map:
            continue

        country = _normalize_text(record.get("market_country"), "Unknown")
        timezone_name = "Europe/Kyiv" if country == "Ukraine" else "UTC"
        market_key = len(key_map) + 1
        key_map[map_key] = market_key
        rows.append(
            {
                "market_key": market_key,
                "market_name": market_name,
                "exchange": _normalize_text(record.get("market_exchange"), "N/A"),
                "country": country,
                "timezone": timezone_name,
                "trading_hours": "N/A",
                "is_active": True,
            }
        )

    return rows, key_map


def _extract_dim_source(records: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    rows: List[Dict[str, Any]] = []
    key_map: Dict[str, int] = {}

    for record in records:
        source = _normalize_text(record.get("source"), "unknown")
        source_name = _canonical_source(source, record)
        map_key = source_name.lower()
        if map_key in key_map:
            continue

        defaults = SOURCE_METADATA.get(source, {})
        source_key = len(key_map) + 1
        key_map[map_key] = source_key
        rows.append(
            {
                "source_key": source_key,
                "source_name": source_name,
                "parser_type": defaults.get("parser_type", f"{source}_parser"),
                "data_type": _normalize_text(record.get("data_type"), defaults.get("data_type", "unknown")),
                "reliability_rating": defaults.get("reliability_rating", 3),
                "update_frequency": defaults.get("update_frequency", "daily"),
                "api_endpoint": defaults.get("api_endpoint"),
                "is_active": True,
            }
        )

    return rows, key_map


def _extract_dim_currency(records: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    rows: List[Dict[str, Any]] = []
    key_map: Dict[str, int] = {}

    currency_codes = {"USD"}
    for record in records:
        code = record.get("currency")
        if isinstance(code, str) and code.strip():
            currency_codes.add(code.strip().upper())

    for currency_code in sorted(currency_codes):
        currency_key = len(key_map) + 1
        key_map[currency_code] = currency_key
        currency_name, country = CURRENCY_METADATA.get(currency_code, (currency_code, "Unknown"))
        rows.append(
            {
                "currency_key": currency_key,
                "currency_code": currency_code,
                "currency_name": currency_name,
                "country": country,
            }
        )

    return rows, key_map


def _build_fact_rows(records: List[Dict[str, Any]], dim_results: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
    facts: List[Dict[str, Any]] = []

    date_map = dim_results["dim_date"]["key_map"]
    commodity_map = dim_results["dim_commodity"]["key_map"]
    market_map = dim_results["dim_market"]["key_map"]
    source_map = dim_results["dim_source"]["key_map"]
    currency_map = dim_results["dim_currency"]["key_map"]

    for record in records:
        ts = (
            _parse_timestamp(record.get("source_timestamp"))
            or _parse_timestamp(record.get("processed_at"))
            or _parse_timestamp(record.get("extracted_at"))
        )
        if ts is None:
            continue

        date_key = date_map.get(ts.date().isoformat())
        commodity_key = commodity_map.get(_normalize_text(record.get("commodity_name"), "Unknown").lower())
        market_name = _normalize_text(
            record.get("market_name") or record.get("market") or record.get("region"),
            "Unknown Market",
        ).lower()
        market_key = market_map.get(market_name)

        source_key = source_map.get(_canonical_source(_normalize_text(record.get("source"), "unknown"), record).lower())
        currency_code = _normalize_text(record.get("currency"), "USD").upper()
        currency_key = currency_map.get(currency_code)

        price_usd = _safe_float(record.get("price_usd"))
        if not all([date_key, commodity_key, market_key, source_key, currency_key]) or price_usd is None:
            continue

        volume = _safe_float(record.get("volume"))
        facts.append(
            {
                "date_key": date_key,
                "commodity_key": commodity_key,
                "market_key": market_key,
                "source_key": source_key,
                "currency_key": currency_key,
                "open_price": price_usd,
                "close_price": price_usd,
                "high_price": price_usd,
                "low_price": price_usd,
                "volume": volume if volume is not None else 0.0,
                "price_type": _normalize_text(record.get("price_type"), "unknown"),
                "delivery_term": record.get("delivery_term"),
            }
        )

    return facts


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
    dag_id="warehouse_load_dag",
    default_args=default_args,
    description="Load transformed silver data into warehouse dimensions and fact table",
    schedule=None,  # Triggered by transformation_dag completion
    catchup=False,
    tags=["warehouse", "commodity", "daily"],
) as dag:

    @task()
    def load_silver_data() -> Dict[str, Any]:
        """Load latest silver parquet records for each transformed source."""
        handler = StagingHandler(storage_type=_storage_type(), layer="silver")

        all_records: List[Dict[str, Any]] = []
        per_source: Dict[str, Dict[str, Any]] = {}

        for source in SOURCES:
            records = handler.load_latest_records(source)
            for record in records:
                record.setdefault("source", source)
            all_records.extend(records)
            per_source[source] = {
                "record_count": len(records),
                "status": "loaded" if records else "no_data",
            }

        return {
            "status": "loaded" if all_records else "no_data",
            "record_count": len(all_records),
            "sources": per_source,
            "records": all_records,
        }

    @task()
    def upsert_dim_date(silver_data: Dict[str, Any]) -> Dict[str, Any]:
        """Build and upsert date dimension from silver records."""
        rows, key_map = _extract_dim_date(silver_data.get("records", []))
        stats = WarehouseLoader().load_dimension_table(rows, "dim_date", ["calendar_date"])
        return {"table": "dim_date", "rows": len(rows), "key_map": key_map, "stats": stats}

    @task()
    def upsert_dim_commodity(silver_data: Dict[str, Any]) -> Dict[str, Any]:
        """Build and upsert commodity dimension from silver records."""
        rows, key_map = _extract_dim_commodity(silver_data.get("records", []))
        stats = WarehouseLoader().load_dimension_table(rows, "dim_commodity", ["commodity_name"])
        return {"table": "dim_commodity", "rows": len(rows), "key_map": key_map, "stats": stats}

    @task()
    def upsert_dim_market(silver_data: Dict[str, Any]) -> Dict[str, Any]:
        """Build and upsert market dimension from silver records."""
        rows, key_map = _extract_dim_market(silver_data.get("records", []))
        stats = WarehouseLoader().load_dimension_table(rows, "dim_market", ["market_name"])
        return {"table": "dim_market", "rows": len(rows), "key_map": key_map, "stats": stats}

    @task()
    def upsert_dim_source(silver_data: Dict[str, Any]) -> Dict[str, Any]:
        """Build and upsert source dimension from silver records."""
        rows, key_map = _extract_dim_source(silver_data.get("records", []))
        stats = WarehouseLoader().load_dimension_table(rows, "dim_source", ["source_name"])
        return {"table": "dim_source", "rows": len(rows), "key_map": key_map, "stats": stats}

    @task()
    def upsert_dim_currency(silver_data: Dict[str, Any]) -> Dict[str, Any]:
        """Build and upsert currency dimension from silver records."""
        rows, key_map = _extract_dim_currency(silver_data.get("records", []))
        stats = WarehouseLoader().load_dimension_table(rows, "dim_currency", ["currency_code"])
        return {"table": "dim_currency", "rows": len(rows), "key_map": key_map, "stats": stats}

    @task()
    def insert_fact_table(silver_data: Dict[str, Any], dim_results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Build and insert fact rows with resolved dimension keys."""
        dim_by_table = {item["table"]: item for item in dim_results}
        fact_rows = _build_fact_rows(silver_data.get("records", []), dim_by_table)
        stats = WarehouseLoader().load_fact_table(
            fact_rows,
            table_name="commodity_prices_fact",
            partition_column="date_key",
        )
        return {
            "table": "commodity_prices_fact",
            "records_prepared": len(fact_rows),
            "inserted": stats.get("inserted", 0),
            "skipped": stats.get("skipped", 0),
            "errors": stats.get("errors", 0),
        }

    @task()
    def refresh_aggregates(fact_results: Dict[str, Any]) -> Dict[str, Any]:
        """Refresh aggregate warehouse tables when fact load succeeds."""
        if fact_results.get("records_prepared", 0) == 0:
            return {"status": "skipped", "reason": "no fact rows prepared", "refreshed": [], "errors": []}

        stats = WarehouseLoader().refresh_aggregate_tables()
        return {
            "status": "completed" if not stats.get("errors") else "partial",
            "refreshed": stats.get("refreshed", []),
            "errors": stats.get("errors", []),
        }

    @task()
    def verify_warehouse(
        silver_data: Dict[str, Any],
        dim_results: List[Dict[str, Any]],
        fact_results: Dict[str, Any],
        agg_results: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Run consistency checks for warehouse loading outputs."""
        checks = {
            "silver_data_present": silver_data.get("record_count", 0) >= 0,
            "dimension_rows_built": all(result.get("rows", 0) > 0 for result in dim_results),
            "fact_records_prepared": fact_results.get("records_prepared", 0) >= fact_results.get("inserted", 0),
            "fact_load_errors_zero": fact_results.get("errors", 0) == 0,
            "aggregate_refresh_errors_zero": len(agg_results.get("errors", [])) == 0,
        }
        passed = sum(1 for value in checks.values() if value)

        return {
            "status": "verified" if passed == len(checks) else "warning",
            "checks_passed": passed,
            "checks_total": len(checks),
            "checks": checks,
            "silver_records": silver_data.get("record_count", 0),
            "fact_inserted": fact_results.get("inserted", 0),
        }

    silver = load_silver_data()

    dim_date = upsert_dim_date(silver)
    dim_commodity = upsert_dim_commodity(silver)
    dim_market = upsert_dim_market(silver)
    dim_source = upsert_dim_source(silver)
    dim_currency = upsert_dim_currency(silver)

    dims = [dim_date, dim_commodity, dim_market, dim_source, dim_currency]
    fact = insert_fact_table(silver, dims)
    agg = refresh_aggregates(fact)
    verify_warehouse(silver, dims, fact, agg)
