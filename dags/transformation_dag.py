"""
Transformation DAG - Transform and clean staged raw data.
Phase 3: Data Pipeline Orchestration (Transformation)

Schedule: Daily after extraction completes
Depends on: extraction_dag

Flow:
  1. Load staged data per source (currency, yfinance, graintradecomua, tripoli_land)
  2. Extract FX rates from currency data (shared dependency)
  3. [Parallel] Clean each commodity source (remove duplicates, nulls, validate)
  4. [Parallel] Normalize each commodity source (prices, units, commodity names, timestamps)
  5. [Parallel] Enrich each source (add dimensions, market info, price types)
  6. [Parallel] Save each source to silver layer as Parquet
"""

import logging
import sys
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Any
from pathlib import Path

from airflow.sdk import DAG, task

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import settings, Environment  # noqa: E402
from staging.staging_handler import StagingHandler  # noqa: E402

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


DEFAULT_CURRENCY_FX_RATE = 43.0  # Default USD/UAH rate if currency data is missing

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
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
    start_date=datetime(2025, 1, 1),
    tags=["transformation", "commodity", "daily"],
) as dag:

    def _load_source_records(source: str) -> List[Dict]:
        """Load latest staged records for a single source from bronze storage."""
        handler = StagingHandler(storage_type="minio", layer="bronze")
        records = handler.load_latest_records(source)

        if not records:
            logger.warning(f"No staged data found for {source}")
            return []

        logger.info(f"Loaded {len(records)} records from {source}")
        return records

    @task()
    def load_currency_data() -> List[Dict]:
        """Load latest staged currency records."""
        return _load_source_records("currency")

    @task()
    def load_yfinance_data() -> List[Dict]:
        """Load latest staged YFinance records."""
        return _load_source_records("yfinance")

    @task()
    def load_graintradecomua_data() -> List[Dict]:
        """Load latest staged GrainTrade UA records."""
        return _load_source_records("graintradecomua")

    @task()
    def load_tripoli_land_data() -> List[Dict]:
        """Load latest staged Tripoli Land records."""
        return _load_source_records("tripoli_land")

    @task()
    def extract_fx_rates(currency_records: Any) -> Dict[str, float]:
        """
        Extract FX rates from currency source data.
        Focus on USD/UAH pair for Ukrainian commodity conversions.

        Args:
            currency_records: Currency records from staging

        Returns:
            Dict mapping "USD_UAH" → rate (e.g., {"USD_UAH": 37.0})
        """
        if not currency_records:
            logger.warning("No currency data found, using default FX rates")
            return {"USD_UAH": DEFAULT_CURRENCY_FX_RATE}  # Default fallback

        logger.info(
            f"Extracting FX rates from {len(currency_records)} currency records"
        )

        fx_rates = {}

        for record in currency_records:
            logger.info(f"Currency record: {record}")
            provider = record.get("provider", "").lower()
            if provider != "nbu":
                continue  # Focus on NBU rates for reliability
            base = record.get("base_currency")
            quote = record.get("quote_currency")
            rate = record.get("rate")
            logger.info(
                f"Currency record: {base}/{quote} = {rate} (provider: {provider})"
            )

            if base and quote and rate is not None:
                key = f"{base}_{quote}"
                # Take latest rate (records should be from single extraction)
                fx_rates[key] = rate
                logger.info(f"Extracted: 1 {base} = {rate} {quote}")

        if "USD_UAH" not in fx_rates and "UAH_USD" in fx_rates and fx_rates["UAH_USD"]:
            fx_rates["USD_UAH"] = 1 / fx_rates["UAH_USD"]
            logger.info("Derived USD_UAH from UAH_USD inverse rate")

        if "USD_UAH" not in fx_rates:
            fx_rates["USD_UAH"] = DEFAULT_CURRENCY_FX_RATE
            logger.warning(
                f"USD_UAH not found in currency data, falling back to {DEFAULT_CURRENCY_FX_RATE}"
            )

        logger.info(f"FX Rate USD/UAH: {fx_rates['USD_UAH']}")

        return fx_rates

    @task()
    def clean_yfinance(raw_data: Any, fx_rates: Any) -> List[Dict]:
        """
        Clean YFinance futures data:
        - Remove duplicates by (ticker, extracted_at)
        - Filter invalid prices (note != 'ok' or price <= 0)
        - Validate required fields
        """
        from transformation.cleaner import DataCleaner

        if not raw_data:
            logger.info("No YFinance data to clean")
            return []

        logger.info(
            f"Using USD/UAH FX rate for pipeline ordering: {fx_rates.get('USD_UAH')}"
        )

        cleaner = DataCleaner("grain")

        # Remove records with note != 'ok'
        filtered = [
            r
            for r in raw_data
            if r.get("note") == "ok"
            and r.get("usd_per_ton", None)
            is not None  # The shares prices defined as null or missing are not valid for our use case
        ]
        logger.info(
            f"YFinance: {len(raw_data)} → {len(filtered)} after validity filtering"
        )

        # Deduplicate by ticker + date
        cleaned = cleaner.remove_duplicates(filtered, ["ticker", "extracted_at"])

        logger.info(f"YFinance cleaned: {len(cleaned)} records")
        return cleaned

    @task()
    def clean_graintradecomua(raw_data: Any, fx_rates: Any) -> List[Dict]:
        """
        Clean GrainTrade UA spot market data:
        - Remove duplicates by (company, crop, offer_date, offer_type)
        - Convert volume_tons to float
        - Filter invalid records (note != 'ok')
        """
        from transformation.cleaner import DataCleaner

        if not raw_data:
            logger.info("No GrainTrade UA data to clean")
            return []

        logger.info(
            f"Using USD/UAH FX rate for pipeline ordering: {fx_rates.get('USD_UAH')}"
        )

        cleaner = DataCleaner("grain")

        # Convert volume to numeric
        for record in raw_data:
            if "volume_tons" in record and isinstance(record["volume_tons"], str):
                try:
                    record["volume_tons"] = float(record["volume_tons"])
                except ValueError:
                    record["volume_tons"] = None

        # Filter valid records
        filtered = [
            r for r in raw_data if r.get("note") == "ok" and r.get("price_value", 0) > 0
        ]
        logger.info(
            f"GrainTrade UA: {len(raw_data)} → {len(filtered)} after validity filtering"
        )

        # Deduplicate
        cleaned = cleaner.remove_duplicates(
            filtered, ["company", "crop", "offer_date", "offer_type"]
        )

        logger.info(f"GrainTrade UA cleaned: {len(cleaned)} records")
        return cleaned

    @task()
    def clean_tripoli_land(raw_data: Any, fx_rates: Any) -> List[Dict]:
        """
        Clean Tripoli Land terminal/elevator prices:
        - Remove duplicates by (company_slug, culture_ua, extracted_at)
        - Filter invalid prices (price_uah_per_ton <= 0)
        - Split combined price field if needed (price → price_uah_per_ton + price_usd_per_ton)
        """
        from transformation.cleaner import DataCleaner

        if not raw_data:
            logger.info("No Tripoli Land data to clean")
            return []

        logger.info(
            f"Using USD/UAH FX rate for pipeline ordering: {fx_rates.get('USD_UAH')}"
        )

        cleaner = DataCleaner("grain")

        # Convert prices to float
        for record in raw_data:
            if record["price_uah_per_ton"] is None and "price" in record:
                price_str = record["price"]
                if isinstance(price_str, str) and price_str.endswith("$"):
                    try:
                        record["price_usd_per_ton"] = float(price_str[-4:].rstrip("$"))
                        record["price"] = price_str[:-4].strip()
                    except ValueError:
                        record["price_usd_per_ton"] = None
                else:
                    try:
                        record["price_uah_per_ton"] = float(price_str.replace(",", ""))
                    except ValueError:
                        record["price_uah_per_ton"] = None

        # Filter valid records
        filtered = [r for r in raw_data if r.get("price_uah_per_ton", 0) > 0]
        logger.info(
            f"Tripoli Land: {len(raw_data)} → {len(filtered)} after validity filtering"
        )

        # Deduplicate
        cleaned = cleaner.remove_duplicates(
            filtered, ["company_slug", "culture_ua", "extracted_at"]
        )

        logger.info(f"Tripoli Land cleaned: {len(cleaned)} records")
        return cleaned

    @task()
    def normalize_yfinance(cleaned_data: Any, fx_rates: Any) -> List[Dict]:
        """
        Normalize YFinance futures data using comprehensive normalizer.
        """
        from transformation.normalizer import DataNormalizer

        if not cleaned_data:
            return []

        normalizer = DataNormalizer(fx_rates)
        normalized = normalizer.normalize_yfinance_data(cleaned_data)

        logger.info(f"YFinance normalized: {len(normalized)} records")
        return normalized

    @task()
    def normalize_graintradecomua(cleaned_data: Any, fx_rates: Any) -> List[Dict]:
        """
        Normalize GrainTrade UA spot market data using comprehensive normalizer.
        """
        from transformation.normalizer import DataNormalizer

        if not cleaned_data:
            return []

        normalizer = DataNormalizer(fx_rates)
        normalized = normalizer.normalize_graintradecomua_data(cleaned_data, fx_rates)

        logger.info(f"GrainTrade UA normalized: {len(normalized)} records")
        return normalized

    @task()
    def normalize_tripoli_land(cleaned_data: Any, fx_rates: Any) -> List[Dict]:
        """
        Normalize Tripoli Land storage price data using comprehensive normalizer.
        """
        from transformation.normalizer import DataNormalizer

        if not cleaned_data:
            return []

        normalizer = DataNormalizer(fx_rates)
        normalized = normalizer.normalize_tripoli_land_data(cleaned_data, fx_rates)

        logger.info(f"Tripoli Land normalized: {len(normalized)} records")
        return normalized

    @task()
    def enrich_yfinance(normalized_data: Any) -> List[Dict]:
        """
        Enrich YFinance futures with business context.
        Data is already normalized, just add market context.
        """
        from transformation.enricher import DataEnricher

        if not normalized_data:
            return []

        enricher = DataEnricher()

        # Add date dimensions using processed_at or source_timestamp
        enriched = enricher.add_date_dimensions(
            normalized_data, date_column="processed_at"
        )

        # Add market and source info (override any existing)
        for record in enriched:
            record["market_name"] = f"CBOT {record.get('commodity_name', 'Unknown')}"
            record["market_exchange"] = "CBOT"
            record["market_country"] = "US"
            record["source_name"] = "Yahoo Finance"
            record["data_type"] = "futures"

        logger.info(f"YFinance enriched: {len(enriched)} records")
        return enriched

    @task()
    def enrich_graintradecomua(normalized_data: Any) -> List[Dict]:
        """
        Enrich GrainTrade UA spot market with business context.
        Data is already normalized, just add market context.
        """
        from transformation.enricher import DataEnricher

        if not normalized_data:
            return []

        enricher = DataEnricher()

        # Add date dimensions
        enriched = enricher.add_date_dimensions(
            normalized_data, date_column="processed_at"
        )

        # Add market and source info
        for record in enriched:
            record["market_name"] = "GrainTrade UA"
            record["market_exchange"] = "GrainTrade"
            record["market_country"] = "Ukraine"
            record["source_name"] = "GrainTrade UA"
            record["data_type"] = "spot_market"

        logger.info(f"GrainTrade UA enriched: {len(enriched)} records")
        return enriched

    @task()
    def enrich_tripoli_land(normalized_data: Any) -> List[Dict]:
        """
        Enrich Tripoli Land storage prices with business context.
        Data is already normalized, just add market context.
        """
        from transformation.enricher import DataEnricher

        if not normalized_data:
            return []

        enricher = DataEnricher()

        # Add date dimensions
        enriched = enricher.add_date_dimensions(
            normalized_data, date_column="processed_at"
        )

        # Add market and source info
        for record in enriched:
            company = record.get("company", "Unknown")
            facility = record.get("facility", "")
            record["market_name"] = f"{company} - {facility}" if facility else company
            record["market_exchange"] = "Tripoli Land"
            record["market_country"] = "Ukraine"
            record["source_name"] = "Tripoli Land"
            record["data_type"] = "storage_rates"

        logger.info(f"Tripoli Land enriched: {len(enriched)} records")
        return enriched

    @task()
    def save_silver_yfinance(enriched_data: Any) -> Dict[str, Any]:
        """Save YFinance data to silver layer (Parquet)."""
        if not enriched_data:
            return {"source": "yfinance", "status": "skipped", "record_count": 0}

        return _save_to_silver("yfinance", enriched_data)

    @task()
    def save_silver_graintradecomua(enriched_data: Any) -> Dict[str, Any]:
        """Save GrainTrade UA data to silver layer (Parquet)."""
        if not enriched_data:
            return {"source": "graintradecomua", "status": "skipped", "record_count": 0}

        return _save_to_silver("graintradecomua", enriched_data)

    @task()
    def save_silver_tripoli_land(enriched_data: Any) -> Dict[str, Any]:
        """Save Tripoli Land data to silver layer (Parquet)."""
        if not enriched_data:
            return {"source": "tripoli_land", "status": "skipped", "record_count": 0}

        return _save_to_silver("tripoli_land", enriched_data)

    def _save_to_silver(source: str, data: List[Dict]) -> Dict[str, Any]:
        """Save enriched data to silver layer using a storage service."""
        if not data:
            return {"source": source, "status": "empty", "record_count": 0}

        storage_type = "hetzner" if settings.env == Environment.PROD else "minio"
        handler = StagingHandler(storage_type=storage_type, layer="silver")
        staged_path = handler.stage_raw_data(
            data=data, source_name=source, file_format="parquet"
        )

        return {
            "source": source,
            "status": "saved",
            "record_count": len(data),
            "path": staged_path,
            "timestamp": datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S"),
        }

    # Task dependencies and orchestration
    currency_data = load_currency_data()
    yfinance_data = load_yfinance_data()
    graintradecomua_data = load_graintradecomua_data()
    tripoli_land_data = load_tripoli_land_data()

    # FX extraction is the first shared prerequisite for the rest of the flow.
    fx_rates = extract_fx_rates(currency_data)

    # Parallel cleaning
    yf_clean = clean_yfinance(yfinance_data, fx_rates)
    gt_clean = clean_graintradecomua(graintradecomua_data, fx_rates)
    tl_clean = clean_tripoli_land(tripoli_land_data, fx_rates)

    # Parallel normalization (depends on FX rates)
    yf_norm = normalize_yfinance(yf_clean, fx_rates)
    gt_norm = normalize_graintradecomua(gt_clean, fx_rates)
    tl_norm = normalize_tripoli_land(tl_clean, fx_rates)

    # Parallel enrichment
    yf_enrich = enrich_yfinance(yf_norm)
    gt_enrich = enrich_graintradecomua(gt_norm)
    tl_enrich = enrich_tripoli_land(tl_norm)

    # Parallel save to silver
    save_silver_yfinance(yf_enrich)
    save_silver_graintradecomua(gt_enrich)
    save_silver_tripoli_land(tl_enrich)
