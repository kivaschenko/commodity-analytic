"""
Extraction DAG - Daily extraction of commodity prices from all sources.
Phase 5: Data Pipeline Orchestration (Extraction)

Schedule: Daily at midnight UTC (0 0 * * *)
"""

from datetime import datetime, timedelta
from pathlib import Path
import sys
from airflow.sdk import DAG, task

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "email": ["airflow@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="extraction_dag",
    default_args=default_args,
    description="Daily extraction of commodity prices from multiple sources",
    schedule="0 0 * * *",  # Daily at midnight UTC
    catchup=False,
    tags=["extraction", "commodity", "daily"],
) as dag:

    @task()
    def extract_yfinance():
        """Extract commodity prices from Yahoo Finance."""
        from parser_services.yfinance_parser import YFinanceParser

        parser = YFinanceParser(storage_type="minio")
        return parser.parse_and_stage(storage_type="minio")

    @task()
    def extract_graintradecomua():
        """Extract commodity prices from GrainTrade.com.ua."""
        from parser_services.graintradecomua_parser import GrainTradeComUaParser

        parser = GrainTradeComUaParser(storage_type="minio")
        return parser.parse_and_stage(storage_type="minio")

    @task()
    def extract_apkinform():
        """Extract commodity prices from APK Inform."""
        from parser_services.apk_inform_parser import APKInformParser

        parser = APKInformParser(storage_type="minio")
        return parser.parse_and_stage(storage_type="minio")

    @task()
    def extract_currency():
        """Extract currency exchange rates."""
        from parser_services.currency_parser import CurrencyParser

        parser = CurrencyParser(storage_type="minio")
        return parser.parse_and_stage(storage_type="minio")

    @task()
    def extract_tripoli_land():
        """Extract grain offer prices from Tripoli Land."""
        from parser_services.tripoli_land_parser import TripoliLandParser

        parser = TripoliLandParser(storage_type="minio")
        return parser.parse_and_stage(storage_type="minio")

    @task()
    def consolidate_extractions(
        yfinance_data,
        graintradecomua_data,
        apkinform_data,
        currency_data,
        tripoli_land_data,
    ):
        """Consolidate all extracted data."""
        consolidated = {
            "yfinance": yfinance_data,
            "graintradecomua": graintradecomua_data,
            "apkinform": apkinform_data,
            "currency": currency_data,
            "tripoli_land": tripoli_land_data,
        }
        return consolidated

    @task()
    def stage_raw_data(consolidated_data):
        """Stage raw data to staging layer (bronze)."""
        from staging.staging_handler import StagingHandler

        handler = StagingHandler(storage_type="minio")
        per_source = {}
        total_record_count = 0
        active_staged_sources = 0

        for source, extraction_status in consolidated_data.items():
            extraction_status = extraction_status or {}
            parser_status = extraction_status.get("status")
            record_count = extraction_status.get("record_count", 0)
            total_record_count += record_count

            staging_status = {"status": "not_extracted"}
            if parser_status == "success":
                staging_status = handler.get_staging_status(source)
                if staging_status.get("status") == "active":
                    active_staged_sources += 1

            per_source[source] = {
                "parser_status": parser_status,
                "record_count": record_count,
                "staging_status": staging_status.get("status"),
                "latest_file": staging_status.get("latest_file"),
                "staged_path": extraction_status.get("staged_path"),
            }

        overall_status = "staged" if active_staged_sources > 0 else "no_data"

        return {
            "status": overall_status,
            "total_record_count": total_record_count,
            "active_staged_sources": active_staged_sources,
            "source_count": len(per_source),
            "sources": per_source,
        }

    # Task dependencies
    yfinance = extract_yfinance()
    graintradecomua = extract_graintradecomua()
    apkinform = extract_apkinform()
    currency = extract_currency()
    tripoli_land = extract_tripoli_land()

    consolidated = consolidate_extractions(
        yfinance, 
        graintradecomua, apkinform, currency, tripoli_land
    )
    staged = stage_raw_data(consolidated)
