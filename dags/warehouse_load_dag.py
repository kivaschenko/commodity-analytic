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
import psycopg2
from psycopg2.extras import execute_values
from airflow.sdk import DAG, task

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import Environment, settings  # noqa: E402
from staging.staging_handler import StagingHandler  # noqa: E402

logger = logging.getLogger(__name__)

SOURCES = ["yfinance", "graintradecomua", "tripoli_land"]
FX_SOURCE = "currency"
FX_ALLOWED_BASE_CURRENCIES = ("USD", "EUR")
FX_QUOTE_CURRENCY = "UAH"


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


def _create_spark_session():
    """Create or retrieve a Spark session configured for warehouse loading."""
    from pyspark.sql import SparkSession
    _parse_database_url(settings.database_url)

    return (
        SparkSession.builder.appName("warehouse_load_dag")
        .master("local[*]")
        # ---------- tolerance for malformed datetime strings ----------
        .config("spark.sql.ansi.enabled", "false")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        # ---------- JDBC driver ----------------------------------
        .config(
            "spark.jars.packages",
            "org.postgresql:postgresql:42.7.3",
        )
        .config("spark.driver.extraJavaOptions", "-Duser.timezone=UTC")
        .config("spark.executor.extraJavaOptions", "-Duser.timezone=UTC")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def _safe_ts(col_expr):
    """
    Parse a string column to TIMESTAMP tolerating multiple real-world formats.
    Returns NULL instead of raising for unrecognised values.
    """
    from pyspark.sql import functions as F

    raw = F.trim(col_expr.cast("string"))
    return (
        F.when(raw.rlike(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}"), F.to_timestamp(raw, "yyyy-MM-dd'T'HH:mm:ss"))
        .when(raw.rlike(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}"), F.to_timestamp(raw, "yyyy-MM-dd HH:mm:ss"))
        .when(raw.rlike(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}$"), F.to_timestamp(raw, "yyyy-MM-dd HH:mm"))
        .when(raw.rlike(r"^\d{2}\.\d{2}\.\d{4} \d{2}:\d{2}:\d{2}"), F.to_timestamp(raw, "dd.MM.yyyy HH:mm:ss"))
        .when(raw.rlike(r"^\d{2}\.\d{2}\.\d{4} \d{2}:\d{2}$"), F.to_timestamp(raw, "dd.MM.yyyy HH:mm"))
        .when(raw.rlike(r"^\d{2}\.\d{2}\.\d{4}$"), F.to_timestamp(raw, "dd.MM.yyyy"))
        .when(raw.rlike(r"^\d{4}-\d{2}-\d{2}$"), F.to_timestamp(raw, "yyyy-MM-dd"))
        .otherwise(F.lit(None).cast("timestamp"))
    )


def _normalize_silver_df(df, source_name: str):
    from pyspark.sql import functions as F

    fallback_source_name = source_name.replace("_", " ").title().replace("Comua", "ComUA")
    columns = set(df.columns)

    def existing_or_null(name: str):
        return F.col(name) if name in columns else F.lit(None).cast("string")

    def coalesce_existing(*names: str, fallback):
        exprs = [F.col(n) for n in names if n in columns]
        exprs.append(fallback)
        return F.coalesce(*exprs)

    # resolve event timestamp safely from whichever column exists
    ts_candidates = [c for c in ("source_timestamp", "processed_at", "extracted_at") if c in columns]
    if ts_candidates:
        event_ts = F.coalesce(*[_safe_ts(F.col(c)) for c in ts_candidates])
    else:
        event_ts = F.lit(None).cast("timestamp")

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
        .withColumn("event_ts", event_ts)
        .withColumn("calendar_date", F.to_date(F.col("event_ts")))
        .filter(
            F.col("price_usd").isNotNull()
            & (F.col("price_usd") > F.lit(0.0))
            & F.col("calendar_date").isNotNull()
        )
        .select(
            "source", "source_name", "data_type", "commodity_name", "grade",
            "market_name", "market_exchange", "market_country",
            "currency_code", "price_usd", "volume",
            "price_type", "delivery_term", "calendar_date",
        )
    )


def _build_dim_date(silver_df):
    from pyspark.sql import functions as F

    return (
        silver_df.select(F.col("calendar_date"))
        .where(F.col("calendar_date").isNotNull())
        .distinct()
        .withColumn("year", F.year("calendar_date"))
        .withColumn("month", F.month("calendar_date"))
        .withColumn("day", F.dayofmonth("calendar_date"))
        .withColumn("quarter", F.quarter("calendar_date"))
        .withColumn("week_of_year", F.weekofyear("calendar_date"))
        .withColumn("day_of_week", F.dayofweek("calendar_date"))
        .withColumn("is_weekend", F.col("day_of_week").isin(1, 7))
        # generate YYYYMMDD integer key
        .withColumn("date_key", F.date_format("calendar_date", "yyyyMMdd").cast("int"))
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
        )
    )


def _build_fx_rates_df(currency_df):
    """Build daily FX rows for dim_exchange_rate from raw currency records."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    columns = set(currency_df.columns)

    def existing_or_null(name: str):
        return F.col(name) if name in columns else F.lit(None).cast("string")

    prepared = (
        currency_df.withColumn("provider_norm", F.lower(F.trim(existing_or_null("provider"))))
        .withColumn("note_norm", F.lower(F.trim(existing_or_null("note"))))
        .withColumn("base_currency", F.upper(F.trim(existing_or_null("base_currency"))))
        .withColumn("quote_currency", F.upper(F.trim(existing_or_null("quote_currency"))))
        .withColumn("exchange_rate", existing_or_null("rate").cast("double"))
        .withColumn("event_ts", F.coalesce(_safe_ts(existing_or_null("source_timestamp")), _safe_ts(existing_or_null("extracted_at"))))
        .withColumn("calendar_date", F.to_date(F.col("event_ts")))
        .withColumn("source", F.lit("NBU"))
        .filter(F.col("provider_norm") == F.lit("nbu"))
        .filter(F.col("note_norm") == F.lit("ok"))
        .filter(F.col("base_currency").isin(*FX_ALLOWED_BASE_CURRENCIES))
        .filter(F.col("quote_currency") == F.lit(FX_QUOTE_CURRENCY))
        .filter(F.col("exchange_rate").isNotNull() & (F.col("exchange_rate") > F.lit(0.0)))
        .filter(F.col("calendar_date").isNotNull())
    )

    row_window = Window.partitionBy("calendar_date", "base_currency", "quote_currency").orderBy(
        F.col("event_ts").desc_nulls_last()
    )

    return (
        prepared.withColumn("rn", F.row_number().over(row_window))
        .filter(F.col("rn") == 1)
        .withColumn("date_key", F.date_format("calendar_date", "yyyyMMdd").cast("int"))
        .select("date_key", "calendar_date", "base_currency", "quote_currency", "exchange_rate", "source")
    )


def _upsert_dim_exchange_rate(spark, jdbc: Dict[str, str], fx_rates_df) -> Dict[str, int]:
    """
    Upsert dim_exchange_rate by natural key (date_key, base_currency, quote_currency).
    - Insert new natural keys.
    - Update exchange_rate/source for existing natural keys when values change.
    """
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    incoming = fx_rates_df.select(
        "date_key", "base_currency", "quote_currency", "exchange_rate", "source"
    ).dropDuplicates(["date_key", "base_currency", "quote_currency"])

    incoming_count = int(incoming.count())
    if incoming_count == 0:
        return {"inserted": 0, "updated": 0, "skipped": 0}

    existing = _read_table_or_empty(spark, jdbc, "dim_exchange_rate")

    if existing is None:
        start_key = 1
        keyed_df = incoming.withColumn(
            "exchange_rate_key",
            (
                F.row_number().over(
                    Window.orderBy(
                        F.col("date_key"), F.col("base_currency"), F.col("quote_currency")
                    )
                )
                + F.lit(start_key - 1)
            ).cast("int"),
        ).select(
            "exchange_rate_key",
            "date_key",
            "base_currency",
            "quote_currency",
            "exchange_rate",
            "source",
        )
        _write_jdbc_append(keyed_df, jdbc, "dim_exchange_rate")
        return {"inserted": incoming_count, "updated": 0, "skipped": 0}

    key_cols = ["date_key", "base_currency", "quote_currency"]
    existing_keys = existing.select(*key_cols, "exchange_rate", "source")

    new_rows = incoming.join(existing_keys.select(*key_cols), key_cols, "left_anti")
    new_count = int(new_rows.count())

    if new_count > 0:
        start_key = _next_key(existing, "exchange_rate_key")
        keyed_new_rows = new_rows.withColumn(
            "exchange_rate_key",
            (
                F.row_number().over(
                    Window.orderBy(
                        F.col("date_key"), F.col("base_currency"), F.col("quote_currency")
                    )
                )
                + F.lit(start_key - 1)
            ).cast("int"),
        ).select(
            "exchange_rate_key",
            "date_key",
            "base_currency",
            "quote_currency",
            "exchange_rate",
            "source",
        )
        _write_jdbc_append(keyed_new_rows, jdbc, "dim_exchange_rate")

    changed_rows = (
        incoming.alias("n")
        .join(existing_keys.alias("e"), key_cols, "inner")
        .filter(
            (F.round(F.col("n.exchange_rate"), 6) != F.round(F.col("e.exchange_rate").cast("double"), 6))
            | (F.coalesce(F.col("n.source"), F.lit("")) != F.coalesce(F.col("e.source"), F.lit("")))
        )
        .select("n.date_key", "n.base_currency", "n.quote_currency", "n.exchange_rate", "n.source")
    )

    updates = [tuple(row) for row in changed_rows.collect()]
    updated_count = len(updates)

    if updated_count > 0:
        conn_kwargs = _parse_postgres_connection_kwargs(settings.database_url)
        with psycopg2.connect(**conn_kwargs) as conn:
            with conn.cursor() as cursor:
                execute_values(
                    cursor,
                    """
                    UPDATE dim_exchange_rate AS t
                    SET exchange_rate = src.exchange_rate,
                        source = src.source
                    FROM (VALUES %s) AS src(date_key, base_currency, quote_currency, exchange_rate, source)
                    WHERE t.date_key = src.date_key
                      AND t.base_currency = src.base_currency
                      AND t.quote_currency = src.quote_currency
                    """,
                    updates,
                )

    skipped_count = incoming_count - new_count - updated_count
    return {"inserted": new_count, "updated": updated_count, "skipped": skipped_count}

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

        # Keep currency in bronze and load a lightweight parquet snapshot for FX dimension upsert.
        currency_handler = StagingHandler(storage_type=_storage_type(), layer="bronze")
        currency_records = currency_handler.load_latest_records(FX_SOURCE)
        currency_count = len(currency_records)
        currency_path = run_dir / f"{FX_SOURCE}.parquet"

        if currency_count > 0:
            pd.DataFrame(currency_records).to_parquet(currency_path, index=False)
            currency_status = "staged"
        else:
            currency_status = "no_data"

        return {
            "status": "loaded" if total_count > 0 else "no_data",
            "record_count": int(total_count),
            "run_dir": str(run_dir),
            "sources": sources_manifest,
            "currency_rates": {
                "status": currency_status,
                "record_count": int(currency_count),
                "path": str(currency_path),
            },
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

        spark = _create_spark_session()

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

            fx_manifest = silver_manifest.get("currency_rates", {})
            fx_rates_daily_df = None
            if fx_manifest.get("record_count", 0) > 0:
                fx_path = fx_manifest.get("path")
                if fx_path and Path(fx_path).exists():
                    raw_currency_df = spark.read.parquet(fx_path)
                    fx_rates_daily_df = _build_fx_rates_df(raw_currency_df)

            jdbc = _parse_database_url(settings.database_url)

            dim_date_input_df = silver_df.select("calendar_date")
            if fx_rates_daily_df is not None:
                dim_date_input_df = dim_date_input_df.unionByName(
                    fx_rates_daily_df.select("calendar_date"), allowMissingColumns=True
                )

            dim_date_df = _build_dim_date(dim_date_input_df)
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
            fx_upsert_stats = {"inserted": 0, "updated": 0, "skipped": 0}

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

            if fx_rates_daily_df is not None:
                fx_upsert_stats = _upsert_dim_exchange_rate(spark, jdbc, fx_rates_daily_df)

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
                "fx_rates": fx_upsert_stats,
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
