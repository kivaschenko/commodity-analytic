"""
Warehouse Load DAG - Spark-based loading from silver parquet files to warehouse tables.
Phase 5: Data Pipeline Orchestration (Warehouse Loading)

Schedule: Triggered after transformation completes
Depends on: transformation_dag
"""

import logging
import sys
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict
from urllib.parse import unquote, urlparse

import pandas as pd
from airflow.sdk import DAG, task

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import Environment, settings  # noqa: E402
from staging.staging_handler import StagingHandler  # noqa: E402

logger = logging.getLogger(__name__)

SOURCES = ["yfinance", "graintradecomua", "tripoli_land"]


def _storage_type() -> str:
    return "hetzner" if settings.env == Environment.PROD else "minio"


def _parse_database_url(db_url: str) -> Dict[str, Any]:
    """Parse SQLAlchemy/Postgres URL and return Spark JDBC connection details."""
    parsed = urlparse(db_url)
    if not parsed.scheme.startswith("postgresql"):
        raise ValueError(f"Unsupported warehouse driver in DB URL: {parsed.scheme}")

    host = parsed.hostname or "localhost"
    port = parsed.port or 5432
    database = parsed.path.lstrip("/")
    user = unquote(parsed.username or "")
    password = unquote(parsed.password or "")

    return {
        "jdbc_url": f"jdbc:postgresql://{host}:{port}/{database}",
        "user": user,
        "password": password,
        "driver": "org.postgresql.Driver",
    }


def _read_table_or_empty(spark, jdbc: Dict[str, str], table_name: str):
    try:
        return (
            spark.read.format("jdbc")
            .option("url", jdbc["jdbc_url"])
            .option("dbtable", table_name)
            .option("user", jdbc["user"])
            .option("password", jdbc["password"])
            .option("driver", jdbc["driver"])
            .load()
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("Unable to read table %s from JDBC: %s", table_name, exc)
        return None


def _write_jdbc_append(df, jdbc: Dict[str, str], table_name: str) -> None:
    (
        df.write.format("jdbc")
        .mode("append")
        .option("url", jdbc["jdbc_url"])
        .option("dbtable", table_name)
        .option("user", jdbc["user"])
        .option("password", jdbc["password"])
        .option("driver", jdbc["driver"])
        .save()
    )


def _next_key(existing_df, key_col: str) -> int:
    if existing_df is None:
        return 1

    from pyspark.sql import functions as F

    max_value = existing_df.agg(F.max(F.col(key_col)).alias("max_key")).collect()[0]["max_key"]
    return int(max_value) + 1 if max_value is not None else 1


def _normalize_silver_df(df, source_name: str):
    from pyspark.sql import functions as F

    fallback_source_name = source_name.replace("_", " ").title().replace("Comua", "ComUA")

    columns = set(df.columns)

    def existing_or_null(column_name: str):
        return F.col(column_name) if column_name in columns else F.lit(None)

    def coalesce_existing(*column_names: str, fallback):
        expressions = [F.col(name) for name in column_names if name in columns]
        expressions.append(fallback)
        return F.coalesce(*expressions)

    def parse_ts_safe(column_name: str):
        raw = F.trim(existing_or_null(column_name).cast("string"))
        return (
            F.when(raw.rlike(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$"), F.to_timestamp(raw, "yyyy-MM-dd HH:mm:ss"))
            .when(raw.rlike(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}$"), F.to_timestamp(raw, "yyyy-MM-dd HH:mm"))
            .when(raw.rlike(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$"), F.to_timestamp(raw, "yyyy-MM-dd'T'HH:mm:ss"))
            .when(raw.rlike(r"^\d{2}\.\d{2}\.\d{4} \d{2}:\d{2}:\d{2}$"), F.to_timestamp(raw, "dd.MM.yyyy HH:mm:ss"))
            .when(raw.rlike(r"^\d{2}\.\d{2}\.\d{4} \d{2}:\d{2}$"), F.to_timestamp(raw, "dd.MM.yyyy HH:mm"))
            .otherwise(F.lit(None).cast("timestamp"))
        )

    ts_col = F.coalesce(
        parse_ts_safe("source_timestamp"),
        parse_ts_safe("processed_at"),
        parse_ts_safe("extracted_at"),
    )

    return (
        df.withColumn("source", coalesce_existing("source", fallback=F.lit(source_name)))
        .withColumn("source_name", coalesce_existing("source_name", fallback=F.lit(fallback_source_name)))
        .withColumn(
            "commodity_name",
            F.when(
                F.length(F.trim(existing_or_null("commodity_name"))) > 0,
                F.trim(existing_or_null("commodity_name")),
            ).otherwise(F.lit("Unknown")),
        )
        .withColumn(
            "market_name",
            coalesce_existing("market_name", "market", "region", fallback=F.lit("Unknown Market")),
        )
        .withColumn("market_exchange", coalesce_existing("market_exchange", fallback=F.lit("N/A")))
        .withColumn("market_country", coalesce_existing("market_country", fallback=F.lit("Unknown")))
        .withColumn("currency_code", F.upper(coalesce_existing("currency", fallback=F.lit("USD"))))
        .withColumn("price_usd", existing_or_null("price_usd").cast("double"))
        .withColumn("volume", existing_or_null("volume").cast("double"))
        .withColumn("data_type", coalesce_existing("data_type", fallback=F.lit("unknown")))
        .withColumn("price_type", coalesce_existing("price_type", fallback=F.lit("unknown")))
        .withColumn("delivery_term", existing_or_null("delivery_term"))
        .withColumn("grade", existing_or_null("grade"))
        .withColumn("event_ts", ts_col)
        .withColumn("calendar_date", F.to_date(F.col("event_ts")))
        .filter(F.col("price_usd").isNotNull() & (F.col("price_usd") > F.lit(0.0)) & F.col("calendar_date").isNotNull())
        .select(
            "source",
            "source_name",
            "data_type",
            "commodity_name",
            "grade",
            "market_name",
            "market_exchange",
            "market_country",
            "currency_code",
            "price_usd",
            "volume",
            "price_type",
            "delivery_term",
            "calendar_date",
        )
    )


def _build_dim_date(silver_df):
    from pyspark.sql import functions as F

    return (
        silver_df.select("calendar_date")
        .dropDuplicates(["calendar_date"])
        .withColumn("date_key", F.date_format(F.col("calendar_date"), "yyyyMMdd").cast("int"))
        .withColumn("year", F.year(F.col("calendar_date")).cast("int"))
        .withColumn("quarter", F.quarter(F.col("calendar_date")).cast("int"))
        .withColumn("month", F.month(F.col("calendar_date")).cast("int"))
        .withColumn("day", F.dayofmonth(F.col("calendar_date")).cast("int"))
        .withColumn("week_of_year", F.weekofyear(F.col("calendar_date")).cast("int"))
        .withColumn("day_of_week", F.date_format(F.col("calendar_date"), "EEEE"))
        .withColumn("is_weekend", F.dayofweek(F.col("calendar_date")).isin(1, 7))
        .withColumn("is_holiday", F.lit(False))
        .withColumn("trading_status", F.lit("active"))
        .select(
            "date_key",
            "calendar_date",
            "year",
            "quarter",
            "month",
            "day",
            "week_of_year",
            "day_of_week",
            "is_weekend",
            "is_holiday",
            "trading_status",
        )
    )


def _build_dim_commodity(silver_df):
    from pyspark.sql import functions as F

    return (
        silver_df.select("commodity_name", "grade")
        .dropDuplicates(["commodity_name"])
        .withColumn(
            "commodity_type",
            F.when(F.lower(F.col("commodity_name")).isin("wheat", "corn", "barley", "rye", "oats"), F.lit("grain"))
            .when(F.lower(F.col("commodity_name")).isin("soybeans", "sunflower", "rapeseed"), F.lit("oil_crop"))
            .otherwise(F.lit("unknown")),
        )
        .withColumn(
            "category",
            F.when(F.lower(F.col("commodity_name")).isin("wheat", "corn", "barley", "rye", "oats"), F.lit("cereals"))
            .when(F.lower(F.col("commodity_name")).isin("soybeans"), F.lit("legumes"))
            .when(F.lower(F.col("commodity_name")).isin("sunflower", "rapeseed"), F.lit("oil_seeds"))
            .otherwise(F.lit("unknown")),
        )
        .withColumn("unit", F.lit("metric_ton"))
        .withColumn("origin_country", F.lit(None).cast("string"))
        .withColumn("is_active", F.lit(True))
        .select(
            "commodity_name",
            "commodity_type",
            "category",
            "unit",
            "grade",
            "origin_country",
            "is_active",
        )
    )


def _build_dim_market(silver_df):
    from pyspark.sql import functions as F

    return (
        silver_df.select("market_name", "market_exchange", "market_country")
        .dropDuplicates(["market_name"])
        .withColumnRenamed("market_exchange", "exchange")
        .withColumnRenamed("market_country", "country")
        .withColumn("timezone", F.when(F.col("country") == F.lit("Ukraine"), F.lit("Europe/Kyiv")).otherwise(F.lit("UTC")))
        .withColumn("trading_hours", F.lit("N/A"))
        .withColumn("is_active", F.lit(True))
        .select("market_name", "exchange", "country", "timezone", "trading_hours", "is_active")
    )


def _build_dim_source(silver_df):
    from pyspark.sql import functions as F

    return (
        silver_df.select("source", "source_name", "data_type")
        .dropDuplicates(["source_name"])
        .withColumn("parser_type", F.concat(F.col("source"), F.lit("_parser")))
        .withColumn("reliability_rating", F.lit(3))
        .withColumn("update_frequency", F.lit("daily"))
        .withColumn("api_endpoint", F.lit(None).cast("string"))
        .withColumn("is_active", F.lit(True))
        .select(
            "source_name",
            "parser_type",
            "data_type",
            "reliability_rating",
            "update_frequency",
            "api_endpoint",
            "is_active",
        )
    )


def _build_dim_currency(silver_df):
    from pyspark.sql import functions as F

    return (
        silver_df.select(F.col("currency_code").alias("currency_code"))
        .dropDuplicates(["currency_code"])
        .withColumn(
            "currency_name",
            F.when(F.col("currency_code") == F.lit("USD"), F.lit("US Dollar"))
            .when(F.col("currency_code") == F.lit("UAH"), F.lit("Ukrainian Hryvnia"))
            .when(F.col("currency_code") == F.lit("EUR"), F.lit("Euro"))
            .otherwise(F.col("currency_code")),
        )
        .withColumn(
            "country",
            F.when(F.col("currency_code") == F.lit("USD"), F.lit("United States"))
            .when(F.col("currency_code") == F.lit("UAH"), F.lit("Ukraine"))
            .when(F.col("currency_code") == F.lit("EUR"), F.lit("European Union"))
            .otherwise(F.lit("Unknown")),
        )
        .select("currency_code", "currency_name", "country")
    )


def _insert_new_dimension_rows(spark, jdbc: Dict[str, str], dim_df, table_name: str, key_col: str, natural_key_col: str):
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    existing_df = _read_table_or_empty(spark, jdbc, table_name)
    if existing_df is not None:
        dim_df = dim_df.join(existing_df.select(natural_key_col), natural_key_col, "left_anti")

    rows_to_insert = dim_df.count()
    if rows_to_insert == 0:
        return 0

    start_key = _next_key(existing_df, key_col)
    window = Window.orderBy(F.col(natural_key_col))
    keyed_df = dim_df.withColumn(key_col, (F.row_number().over(window) + F.lit(start_key - 1)).cast("int"))

    _write_jdbc_append(keyed_df, jdbc, table_name)
    return int(rows_to_insert)


def _build_fact_df(silver_df, dim_date, dim_commodity, dim_market, dim_source, dim_currency):
    from pyspark.sql import functions as F

    return (
        silver_df.alias("s")
        .join(dim_date.alias("dd"), F.col("s.calendar_date") == F.col("dd.calendar_date"), "inner")
        .join(dim_commodity.alias("dc"), F.col("s.commodity_name") == F.col("dc.commodity_name"), "inner")
        .join(dim_market.alias("dm"), F.col("s.market_name") == F.col("dm.market_name"), "inner")
        .join(dim_source.alias("ds"), F.col("s.source_name") == F.col("ds.source_name"), "inner")
        .join(dim_currency.alias("dcu"), F.col("s.currency_code") == F.col("dcu.currency_code"), "inner")
        .select(
            F.col("dd.date_key").cast("int").alias("date_key"),
            F.col("dc.commodity_key").cast("int").alias("commodity_key"),
            F.col("dm.market_key").cast("int").alias("market_key"),
            F.col("ds.source_key").cast("int").alias("source_key"),
            F.col("dcu.currency_key").cast("int").alias("currency_key"),
            F.col("s.price_usd").cast("double").alias("open_price"),
            F.col("s.price_usd").cast("double").alias("close_price"),
            F.col("s.price_usd").cast("double").alias("high_price"),
            F.col("s.price_usd").cast("double").alias("low_price"),
            F.coalesce(F.col("s.volume"), F.lit(0.0)).cast("double").alias("volume"),
            F.coalesce(F.col("s.price_type"), F.lit("unknown")).alias("price_type"),
            F.col("s.delivery_term").alias("delivery_term"),
        )
        .dropDuplicates(
            [
                "date_key",
                "commodity_key",
                "market_key",
                "source_key",
                "currency_key",
                "price_type",
                "delivery_term",
            ]
        )
    )


def _load_dim_tables(spark, jdbc: Dict[str, str]):
    return {
        "dim_date": _read_table_or_empty(spark, jdbc, "dim_date"),
        "dim_commodity": _read_table_or_empty(spark, jdbc, "dim_commodity"),
        "dim_market": _read_table_or_empty(spark, jdbc, "dim_market"),
        "dim_source": _read_table_or_empty(spark, jdbc, "dim_source"),
        "dim_currency": _read_table_or_empty(spark, jdbc, "dim_currency"),
    }


def _append_fact_rows(spark, jdbc: Dict[str, str], fact_df):
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    existing_fact = _read_table_or_empty(spark, jdbc, "commodity_prices_fact")
    if existing_fact is not None:
        fact_df = fact_df.join(
            existing_fact.select(
                "date_key",
                "commodity_key",
                "market_key",
                "source_key",
                "currency_key",
                "price_type",
                "delivery_term",
            ),
            [
                "date_key",
                "commodity_key",
                "market_key",
                "source_key",
                "currency_key",
                "price_type",
                "delivery_term",
            ],
            "left_anti",
        )

    fact_rows = int(fact_df.count())
    if fact_rows == 0:
        return 0

    start_price_id = _next_key(existing_fact, "price_id")
    keyed_fact_df = fact_df.withColumn(
        "price_id",
        (F.row_number().over(Window.orderBy(F.col("date_key"), F.col("commodity_key"))) + F.lit(start_price_id - 1)).cast("bigint"),
    ).select(
        "price_id",
        "date_key",
        "commodity_key",
        "market_key",
        "source_key",
        "currency_key",
        "open_price",
        "close_price",
        "high_price",
        "low_price",
        "volume",
        "price_type",
        "delivery_term",
    )

    _write_jdbc_append(keyed_fact_df, jdbc, "commodity_prices_fact")
    return fact_rows


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
    description="Spark loading from silver parquet to warehouse dimensions and fact table",
    schedule=None,
    catchup=False,
    tags=["warehouse", "commodity", "daily", "spark"],
) as dag:

    @task()
    def load_silver_data() -> Dict[str, Any]:
        """
        Load the latest silver records per source and persist to local temp parquet files.
        Returns only a lightweight manifest to avoid large XCom payloads.
        """
        handler = StagingHandler(storage_type=_storage_type(), layer="silver")
        run_dir = Path(tempfile.gettempdir()) / "warehouse_silver" / datetime.utcnow().strftime("%Y%m%dT%H%M%S")
        run_dir.mkdir(parents=True, exist_ok=True)

        sources_manifest: Dict[str, Dict[str, Any]] = {}
        total_count = 0

        for source in SOURCES:
            records = handler.load_latest_records(source)
            count = len(records)
            source_path = run_dir / f"{source}.parquet"

            if count > 0:
                pd.DataFrame(records).to_parquet(source_path, index=False)
                status = "staged"
            else:
                status = "no_data"

            sources_manifest[source] = {
                "status": status,
                "record_count": int(count),
                "path": str(source_path),
            }
            total_count += count

        return {
            "status": "loaded" if total_count > 0 else "no_data",
            "record_count": int(total_count),
            "run_dir": str(run_dir),
            "sources": sources_manifest,
        }

    @task()
    def load_warehouse_with_spark(silver_manifest: Dict[str, Any]) -> Dict[str, Any]:
        """Run Spark job that loads dimensions and fact table into warehouse PostgreSQL."""
        if silver_manifest.get("status") != "loaded":
            return {
                "status": "skipped",
                "reason": "no silver data",
                "dim_inserted": {},
                "fact_inserted": 0,
            }

        from pyspark.sql import SparkSession

        spark = (
            SparkSession.builder.appName("warehouse_load_dag")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.shuffle.partitions", "8")
            .getOrCreate()
        )

        try:
            source_dfs = []
            for source in SOURCES:
                source_info = silver_manifest.get("sources", {}).get(source, {})
                if source_info.get("record_count", 0) <= 0:
                    continue

                source_path = source_info.get("path")
                if not source_path or not Path(source_path).exists():
                    continue

                source_df = spark.read.parquet(source_path)
                source_dfs.append(_normalize_silver_df(source_df, source))

            if not source_dfs:
                return {
                    "status": "skipped",
                    "reason": "no readable parquet snapshots",
                    "dim_inserted": {},
                    "fact_inserted": 0,
                }

            silver_df = source_dfs[0]
            for df in source_dfs[1:]:
                silver_df = silver_df.unionByName(df, allowMissingColumns=True)

            silver_df = silver_df.dropDuplicates(
                ["calendar_date", "commodity_name", "market_name", "source_name", "currency_code", "price_type"]
            )

            jdbc = _parse_database_url(settings.database_url)

            dim_date_df = _build_dim_date(silver_df)
            dim_commodity_df = _build_dim_commodity(silver_df)
            dim_market_df = _build_dim_market(silver_df)
            dim_source_df = _build_dim_source(silver_df)
            dim_currency_df = _build_dim_currency(silver_df)

            inserted_counts = {
                "dim_date": 0,
                "dim_commodity": 0,
                "dim_market": 0,
                "dim_source": 0,
                "dim_currency": 0,
            }

            existing_dim_date = _read_table_or_empty(spark, jdbc, "dim_date")
            if existing_dim_date is not None:
                to_insert_date = dim_date_df.join(existing_dim_date.select("calendar_date"), "calendar_date", "left_anti")
            else:
                to_insert_date = dim_date_df

            inserted_counts["dim_date"] = int(to_insert_date.count())
            if inserted_counts["dim_date"] > 0:
                _write_jdbc_append(to_insert_date, jdbc, "dim_date")

            inserted_counts["dim_commodity"] = _insert_new_dimension_rows(
                spark, jdbc, dim_commodity_df, "dim_commodity", "commodity_key", "commodity_name"
            )
            inserted_counts["dim_market"] = _insert_new_dimension_rows(
                spark, jdbc, dim_market_df, "dim_market", "market_key", "market_name"
            )
            inserted_counts["dim_source"] = _insert_new_dimension_rows(
                spark, jdbc, dim_source_df, "dim_source", "source_key", "source_name"
            )
            inserted_counts["dim_currency"] = _insert_new_dimension_rows(
                spark, jdbc, dim_currency_df, "dim_currency", "currency_key", "currency_code"
            )

            dims = _load_dim_tables(spark, jdbc)
            if any(value is None for value in dims.values()):
                raise RuntimeError("One or more dimension tables are not readable after dimension loading")

            fact_df = _build_fact_df(
                silver_df,
                dims["dim_date"],
                dims["dim_commodity"],
                dims["dim_market"],
                dims["dim_source"],
                dims["dim_currency"],
            )
            fact_rows = _append_fact_rows(spark, jdbc, fact_df)

            return {
                "status": "loaded",
                "silver_records": int(silver_manifest.get("record_count", 0)),
                "dim_inserted": inserted_counts,
                "fact_inserted": int(fact_rows),
            }
        finally:
            spark.stop()

    @task()
    def verify_warehouse_load(load_stats: Dict[str, Any]) -> Dict[str, Any]:
        """Basic verification summary for Airflow monitoring."""
        if load_stats.get("status") == "skipped":
            return {
                "status": "skipped",
                "checks_passed": 1,
                "checks_total": 1,
                "checks": {"no_data_run": True},
            }

        checks = {
            "warehouse_load_success": load_stats.get("status") == "loaded",
            "fact_loaded_or_empty_valid": load_stats.get("fact_inserted", 0) >= 0,
            "dim_result_present": isinstance(load_stats.get("dim_inserted"), dict),
        }
        passed = sum(1 for value in checks.values() if value)

        return {
            "status": "verified" if passed == len(checks) else "warning",
            "checks_passed": passed,
            "checks_total": len(checks),
            "checks": checks,
            "load_stats": load_stats,
        }

    silver = load_silver_data()
    warehouse_stats = load_warehouse_with_spark(silver)
    verify_warehouse_load(warehouse_stats)
