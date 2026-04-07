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
    def extract_investingcom():
        """Extract commodity prices from Investing.com."""
        # TODO: Implement Investing.com extraction
        # from parser_services.investingcom_parser import InvestingComParser
        # parser = InvestingComParser()
        # data = parser.parse()
        # return data
        return {"status": "pending", "source": "investingcom"}

    @task()
    def extract_graintradecomua():
        """Extract commodity prices from GrainTrade.com.ua."""
        # TODO: Implement GrainTrade extraction
        # from parser_services.graintradecomua_parser import GrainTradeComUAParser
        # parser = GrainTradeComUAParser()
        # data = parser.parse()
        # return data
        return {"status": "pending", "source": "graintradecomua"}

    @task()
    def extract_apkinform():
        """Extract commodity prices from APK Inform."""
        # TODO: Implement APK Inform extraction
        # from parser_services.apk_inform_parser import APKInformParser
        # parser = APKInformParser()
        # data = parser.parse()
        # return data
        return {"status": "pending", "source": "apkinform"}

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
        investingcom_data,
        graintradecomua_data,
        apkinform_data,
        currency_data,
        tripoli_land_data,
    ):
        """Consolidate all extracted data."""
        # TODO: Combine all data sources
        consolidated = {
            "yfinance": yfinance_data,
            "investingcom": investingcom_data,
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

        yfinance_status = consolidated_data.get("yfinance", {})
        handler = StagingHandler(storage_type="minio")
        staging_status = handler.get_staging_status("yfinance")

        return {
            "status": "staged" if staging_status.get("status") == "active" else "no_data",
            "record_count": yfinance_status.get("record_count", 0),
            "latest_file": staging_status.get("latest_file"),
            "source": "yfinance",
            "currency_status": consolidated_data.get("currency", {}),
            "tripoli_land_status": consolidated_data.get("tripoli_land", {}),
        }

    # Task dependencies
    yfinance = extract_yfinance()
    investingcom = extract_investingcom()
    graintradecomua = extract_graintradecomua()
    apkinform = extract_apkinform()
    currency = extract_currency()
    tripoli_land = extract_tripoli_land()

    consolidated = consolidate_extractions(
        yfinance, investingcom, graintradecomua, apkinform, currency, tripoli_land
    )
    staged = stage_raw_data(consolidated)
