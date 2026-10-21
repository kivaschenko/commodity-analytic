"""
Backfill DAG - Backfill historical data and handle recovery.
Phase 5: Data Pipeline Orchestration (Backfill & Recovery)

Manual trigger for backfilling gaps or recovering from failures.
"""
import logging
import sys

from datetime import datetime, timedelta
from pathlib import Path
from airflow.sdk import DAG, task
from airflow.sdk.definitions.param import ParamsDict

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from parser_services.yfinance_parser import YFinanceParser  # noqa: E402

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email": ["civaschenko@yahoo.com"],
    "email_on_failure": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
}

logger = logging.Logger(__name__)

class ParserFactory:
    @staticmethod
    def get_parser(source_name: str):
        if source_name == "yfinance":
            from parser_services.yfinance_parser import YFinanceParser
            return YFinanceParser(storage_type='minio')
        if source_name == "graintradecomua":
            from parser_services.graintradecomua_parser import GrainTradeComUaParser
            return GrainTradeComUaParser(storage_type='minio')
        if source_name == "currency":
            from parser_services.currency_parser import CurrencyParser
            return CurrencyParser(storage_type='minio')
        if source_name == "tripoli_land":
            from parser_services.tripoli_land_parser import TripoliLandParser
            return TripoliLandParser(storage_type='minio')
        # Add other parsers here as needed
        raise ValueError(f"Unsupported source: {source_name}")


with DAG(
    dag_id="backfill_dag",
    default_args=default_args,
    description="Backfill historical data and recover from failures",
    schedule=None,  # Manual trigger
    catchup=False,
    start_date=None,
    end_date=datetime(2026, 12, 31),
    tags=["backfill", "manual", "recovery"],
    params=ParamsDict(
        dict(
            sources=["yfinance", "graintradecomua", "currency", "tripoli_land"],
            start_date="2024-01-01",
            end_date="2026-12-31",
        ),
    ),
) as dag:

    @task()
    def extract_historical_data(
        sources="{{ params.sources }}", start_date="{{ params.start_date }}", end_date="{{ params.end_date }}"
    ):
        """Extract historical data for date range and sources."""
        for source in sources:
            logger.info("Run parsing for historical data of %", source)
            parser = ParserFactory.get_parser(source)
            data = parser.parse_and_stage(start_date=start_date, end_date=end_date)
            logger.info("Fetched %s records of %s historical data. Status: %s. Storage type: %s. Path: %s", 
                        (data['record_count'], data['source'], data['status'], data['storage_type'], data['storage_path']))
        return {
            "status": "extracted",
            "start_date": start_date,
            "end_date": end_date,
            "sources": sources,
        }

    @task()
    def clean_historical_data(extracted_data):
        """Clean and validate historical data."""
        # TODO: Implement cleaning for historical data
        # Use same cleaning pipeline as daily extraction
        return {"status": "cleaned", "record_count": 0}

    @task()
    def transform_historical_data(cleaned_data):
        """Transform historical data."""
        # TODO: Implement transformation for historical data
        # Use same transformation pipeline as daily extraction
        return {"status": "transformed", "record_count": 0}

    @task()
    def load_historical_data(transformed_data):
        """Load historical data into warehouse."""
        # TODO: Implement loading for historical data
        # Use same warehouse loading pipeline
        # Handle duplicate keys (may already exist)
        return {
            "fact_table": "loaded",
            "dimensions": "loaded",
            "record_count": 0,
        }

    @task()
    def refresh_all_aggregates():
        """Refresh all aggregate tables after backfill."""
        # TODO: Refresh aggregate tables that depend on new data
        return {
            "daily_summary": "refreshed",
            "weekly_summary": "refreshed",
            "monthly_summary": "refreshed",
        }

    @task()
    def verify_backfill(load_results):
        """Verify backfill completeness and integrity."""
        # TODO: Run verification
        # - Check row counts match expectations
        # - Verify all dates have data
        # - Check for gaps
        return {"status": "verified", "issues": 0}

    # Task dependencies
    extracted = extract_historical_data()
    cleaned = clean_historical_data(extracted)
    transformed = transform_historical_data(cleaned)
    loaded = load_historical_data(transformed)
    agg_refreshed = refresh_all_aggregates()
    verified = verify_backfill(loaded)
