"""
Transformation DAG - Transform and clean staged raw data.
Phase 5: Data Pipeline Orchestration (Transformation)

Schedule: Daily after extraction completes
Depends on: extraction_dag
"""

from datetime import datetime, timedelta
from airflow.sdk import DAG, task
from airflow.models import Variable

default_args = {
    "owner": "data-engineering",
    "depends_on_past": True,
    "start_date": datetime(2025, 1, 1),
    "email": ["airflow@example.com"],
    "email_on_failure": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="transformation_dag",
    default_args=default_args,
    description="Transform and clean staged commodity price data",
    schedule=None,  # Triggered by extraction_dag completion
    catchup=False,
    tags=["transformation", "commodity", "daily"],
) as dag:

    @task()
    def load_staged_data():
        """Load raw data from staging layer."""
        # TODO: Implement loading from staging
        # from staging import StagingHandler
        # handler = StagingHandler()
        # raw_data = handler.load_staged_data()
        return {"status": "loaded", "record_count": 0}

    @task()
    def data_quality_checks(raw_data):
        """Run data quality checks on staged data."""
        # TODO: Implement quality checks
        # from staging.data_quality import DataQualityChecker
        # checker = DataQualityChecker("staged_data")
        # results = checker.check_row_count(raw_data)
        # if not results.passed:
        #     raise ValueError(f"Data quality check failed: {results.error_message}")
        return {"status": "quality_passed", "checks": 5}

    @task()
    def clean_data(raw_data):
        """Remove duplicates, handle missing values, detect outliers."""
        # TODO: Implement cleaning
        # from transformation.cleaner import DataCleaner
        # cleaner = DataCleaner()
        # cleaned = cleaner.remove_duplicates(raw_data, ["date", "commodity"])
        # cleaned = cleaner.handle_missing_values(cleaned, strategy="drop")
        # cleaned, outliers = cleaner.detect_outliers(cleaned, "price")
        return {"status": "cleaned", "record_count": 0}

    @task()
    def normalize_data(cleaned_data):
        """Normalize prices, units, currencies, timestamps."""
        # TODO: Implement normalization
        # from transformation.normalizer import DataNormalizer
        # normalizer = DataNormalizer()
        # normalized = normalizer.normalize_prices(cleaned_data, base_currency="USD")
        # normalized = normalizer.normalize_units(normalized, target_unit="ton")
        # normalized = normalizer.normalize_timestamps(normalized)
        return {"status": "normalized", "record_count": 0}

    @task()
    def enrich_data(normalized_data):
        """Add business dimensions and calculated fields."""
        # TODO: Implement enrichment
        # from transformation.enricher import DataEnricher
        # enricher = DataEnricher()
        # enriched = enricher.add_date_dimensions(normalized_data)
        # enriched = enricher.add_trading_flags(enriched)
        # enriched = enricher.add_price_changes(enriched)
        # enriched = enricher.add_commodity_attributes(enriched)
        return {"status": "enriched", "record_count": 0}

    @task()
    def save_silver_data(enriched_data):
        """Save cleaned data to silver layer (Parquet)."""
        # TODO: Implement saving to silver layer
        # enriched_data.to_parquet("s3://data-lake/silver/commodity_prices/")
        return {"status": "saved", "path": "s3://data-lake/silver/"}

    # Task dependencies
    staged = load_staged_data()
    quality = data_quality_checks(staged)
    cleaned = clean_data(staged)
    normalized = normalize_data(cleaned)
    enriched = enrich_data(normalized)
    silver = save_silver_data(enriched)
