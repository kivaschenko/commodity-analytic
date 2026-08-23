"""
Data Normalizer - Standardizes data formats and units.
Normalizes prices, units, currencies, timestamps, and commodity names.
"""

import logging
import re
from typing import List, Dict, Any, Optional
from datetime import datetime
from pathlib import Path

# Try to import with relative path for production, fallback to absolute
try:
    from config.transformation_mappings import normalize_crop_name
except ImportError:
    # Fallback implementation if config import fails
    def normalize_crop_name(crop_name):
        """Fallback crop name normalization."""
        if not crop_name:
            return None
        # Simple mappings for basic functionality
        mappings = {
            "пшениця": "Wheat",
            "кукурудза": "Corn",
            "соя": "Soybeans",
            "ячмінь": "Barley",
            "wheat": "Wheat",
            "corn": "Corn",
            "soybeans": "Soybeans",
            "barley": "Barley",
        }
        return mappings.get(crop_name.lower().strip())


logger = logging.getLogger(__name__)


class DataNormalizer:
    """
    Normalizes commodity data to standard formats:
    - Price normalization (convert all to USD/standard unit)
    - Unit standardization (tons, bushels, etc.)
    - Currency conversion with FX rates
    - Commodity name normalization (Ukrainian → English)
    - Timestamp standardization
    - Numeric precision
    """

    def __init__(self, fx_rates: Optional[Dict[str, float]] = None):
        """
        Initialize normalizer with optional FX rates.

        Args:
            fx_rates: Dict mapping "BASE_QUOTE" → rate (e.g., {"USD_UAH": 37.0})
                     If None, uses default rates
        """
        self.normalization_log = []

        # Default exchange rates (should be overridden with actual rates from currency source)
        self.default_exchange_rates = {
            "USD": 1.0,
            "EUR": 1.10,  # EUR to USD
            "UAH": 0.027,  # UAH to USD (default fallback)
            "GBP": 1.27,
        }

        # FX rates as dict: can pass {"USD_UAH": 37.0} or update later
        self.fx_rates = fx_rates or {}
        self.exchange_rates = self.default_exchange_rates.copy()

        # Unit conversions to tons
        self.unit_conversions = {
            "ton": 1.0,
            "tonne": 1.0,
            "bushel": 0.0272155,  # bushel to ton
            "cwt": 0.05,  # hundredweight to ton
            "kg": 0.001,
            "lb": 0.000453592,
        }

    def set_fx_rate(self, base_currency: str, quote_currency: str, rate: float) -> None:
        """
        Set exchange rate for currency pair.

        Args:
            base_currency: From currency (e.g., 'USD')
            quote_currency: To currency (e.g., 'UAH')
            rate: Exchange rate (e.g., 37.0 for 1 USD = 37 UAH)
        """
        self.exchange_rates[f"{base_currency}_{quote_currency}"] = rate
        # Also set the inverse for conversions
        if rate > 0:
            self.exchange_rates[f"{quote_currency}_{base_currency}"] = 1 / rate
        logger.info(f"Set FX rate: 1 {base_currency} = {rate} {quote_currency}")

    def _get_currency_rate(self, from_currency: str, to_currency: str) -> float:
        """
        Get exchange rate between two currencies.

        Args:
            from_currency: Source currency code
            to_currency: Target currency code

        Returns:
            Exchange rate (default 1.0 if not found)
        """
        if from_currency == to_currency:
            return 1.0

        # Try explicit pair first
        pair_key = f"{from_currency}_{to_currency}"
        if pair_key in self.exchange_rates:
            return self.exchange_rates[pair_key]

        # Try base currency rates
        from_rate = self.exchange_rates.get(from_currency, 1.0)
        to_rate = self.exchange_rates.get(to_currency, 1.0)

        if from_rate and to_rate:
            return to_rate / from_rate

        logger.warning(
            f"FX rate not found for {from_currency} → {to_currency}, using 1.0"
        )
        return 1.0

    def normalize_prices(
        self,
        data: List[Dict],
        price_column: str = "price",
        currency_column: str = "currency",
        base_currency: str = "USD",
    ) -> List[Dict]:
        """
        Convert all prices to base currency using FX rates.

        Args:
            data: Records with prices in various currencies
            price_column: Column name with price values
            currency_column: Column name with currency codes
            base_currency: Target currency (default: USD)

        Returns:
            Data with normalized prices
        """
        normalized = []
        conversions_count = 0
        errors = 0

        for record in data:
            normalized_record = record.copy()

            if price_column in record and currency_column in record:
                try:
                    original_price = record[price_column]
                    original_currency = record[currency_column]

                    if original_price is None:
                        errors += 1
                        continue

                    if original_currency != base_currency:
                        rate = self._get_currency_rate(original_currency, base_currency)
                        normalized_record[price_column] = round(
                            original_price * rate, 4
                        )
                        normalized_record[f"{price_column}_currency"] = base_currency
                        conversions_count += 1
                    else:
                        normalized_record[f"{price_column}_currency"] = (
                            original_currency
                        )

                except (ValueError, TypeError) as e:
                    logger.warning(f"Error normalizing price: {e}")
                    errors += 1
                    continue

            normalized.append(normalized_record)

        self.normalization_log.append(
            {
                "operation": "normalize_prices",
                "base_currency": base_currency,
                "conversions": conversions_count,
                "errors": errors,
                "total_records": len(data),
            }
        )

        return normalized

    def normalize_units(
        self,
        data: List[Dict],
        quantity_column: str = "quantity",
        unit_column: str = "unit",
        target_unit: str = "ton",
    ) -> List[Dict]:
        """
        Convert all quantities to target unit.

        Args:
            data: Records with quantities in various units
            quantity_column: Column with quantity values
            unit_column: Column with unit names
            target_unit: Target unit (default: ton)

        Returns:
            Data with normalized quantities
        """
        normalized = []
        conversions_count = 0

        for record in data:
            normalized_record = record.copy()

            if quantity_column in record and unit_column in record:
                original_qty = record[quantity_column]
                original_unit = (
                    record[unit_column].lower().strip()
                    if isinstance(record[unit_column], str)
                    else str(record[unit_column])
                )

                if original_unit != target_unit:
                    conversion_factor = self.unit_conversions.get(original_unit, 1.0)
                    normalized_record[quantity_column] = round(
                        original_qty * conversion_factor, 2
                    )
                    normalized_record[unit_column] = target_unit
                    conversions_count += 1

            normalized.append(normalized_record)

        self.normalization_log.append(
            {
                "operation": "normalize_units",
                "target_unit": target_unit,
                "conversions": conversions_count,
            }
        )

        return normalized

    def normalize_commodity_names(
        self, data: List[Dict], commodity_column: str = "commodity"
    ) -> List[Dict]:
        """
        Normalize commodity names to canonical English form.
        Maps Ukrainian names and variants to canonical names.

        Args:
            data: Records with commodity names in various languages
            commodity_column: Column with commodity names

        Returns:
            Data with normalized commodity names
        """
        normalized = []
        normalized_count = 0
        unmapped_count = 0
        unknown_names = set()

        for record in data:
            normalized_record = record.copy()

            if commodity_column in record:
                raw_name = record[commodity_column]
                canonical_name = normalize_crop_name(raw_name)

                if canonical_name:
                    normalized_record[commodity_column] = canonical_name
                    normalized_record[f"{commodity_column}_source"] = raw_name
                    normalized_count += 1
                else:
                    unmapped_count += 1
                    unknown_names.add(raw_name)
                    logger.debug(f"Could not normalize commodity name: {raw_name}")

            normalized.append(normalized_record)

        self.normalization_log.append(
            {
                "operation": "normalize_commodity_names",
                "normalized": normalized_count,
                "unmapped": unmapped_count,
                "unknown_names": list(unknown_names)[:10],  # Log first 10 unknown names
            }
        )

        return normalized

    def normalize_timestamps(
        self,
        data: List[Dict],
        timestamp_column: str = "date",
        output_format: str = "%Y-%m-%d",
    ) -> List[Dict]:
        """
        Standardize all timestamps to ISO format.
        Handles multiple input formats.

        Args:
            data: Records with various timestamp formats
            timestamp_column: Column with timestamps
            output_format: Target format (default: YYYY-MM-DD)

        Returns:
            Data with normalized timestamps
        """
        normalized = []
        parse_errors = 0

        # Common timestamp formats to try
        formats_to_try = [
            "%Y-%m-%d",
            "%Y/%m/%d",
            "%d-%m-%Y",
            "%d.%m.%Y",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%dT%H:%M:%S.%f",
            "%Y-%m-%dT%H:%M:%S.%f%z",
            "%d.%m.%Y %H:%M",
        ]

        for record in data:
            normalized_record = record.copy()

            if timestamp_column in record:
                ts_value = record[timestamp_column]

                if isinstance(ts_value, str) and ts_value.strip():
                    parsed = False
                    for fmt in formats_to_try:
                        try:
                            parsed_ts = datetime.strptime(ts_value.strip(), fmt)
                            normalized_record[timestamp_column] = parsed_ts.strftime(
                                output_format
                            )
                            parsed = True
                            break
                        except ValueError:
                            continue

                    if not parsed:
                        logger.warning(f"Failed to parse timestamp: {ts_value}")
                        parse_errors += 1

                elif isinstance(ts_value, datetime):
                    normalized_record[timestamp_column] = ts_value.strftime(
                        output_format
                    )

            normalized.append(normalized_record)

        self.normalization_log.append(
            {
                "operation": "normalize_timestamps",
                "target_format": output_format,
                "parse_errors": parse_errors,
                "total_records": len(data),
            }
        )

        return normalized

    def round_numeric_values(
        self, data: List[Dict], numeric_columns: List[str], decimals: int = 2
    ) -> List[Dict]:
        """
        Round numeric values to specified decimal places.

        Args:
            data: Records with numeric values
            numeric_columns: Columns to round
            decimals: Number of decimal places

        Returns:
            Data with rounded values
        """
        normalized = []

        for record in data:
            normalized_record = record.copy()

            for col in numeric_columns:
                if col in record and isinstance(record[col], (int, float)):
                    normalized_record[col] = round(record[col], decimals)

            normalized.append(normalized_record)

        self.normalization_log.append(
            {
                "operation": "round_numeric_values",
                "decimals": decimals,
                "columns": numeric_columns,
            }
        )

        return normalized

    def normalize_yfinance_data(self, data: List[Dict]) -> List[Dict]:
        """
        Normalize YFinance futures data to standard schema.

        Args:
            data: Raw YFinance records

        Returns:
            Normalized records with standard fields
        """
        normalized = []

        for record in data:
            # YFinance data is already in USD per ton, just standardize structure
            normalized_record = {
                "record_id": f"yfinance_{record.get('_staging_id', 'unknown')}",
                "source": "yfinance",
                "commodity_name": record.get("name", "").replace(" Futures", ""),
                "price_usd": record.get("usd_per_ton", 0),
                "price_uah": None,  # Will be calculated if needed TODO: add UAH price if FX rate available
                "volume": None,  # Futures don't have volume in same way
                "price_type": "futures_close",
                "currency": "USD",
                "unit": "metric_ton",
                "market": record.get("description", ""),
                "ticker": record.get("ticker", ""),
                "source_timestamp": record.get("extracted_at"),
                "processed_at": datetime.now().isoformat(),
                "raw_data": record,  # Keep original for reference
            }
            normalized.append(normalized_record)

        self.normalization_log.append(
            {
                "operation": "normalize_yfinance_data",
                "records_processed": len(data),
                "output_records": len(normalized),
            }
        )

        return normalized

    def normalize_graintradecomua_data(
        self, data: List[Dict], fx_rates: Dict[str, float]
    ) -> List[Dict]:
        """
        Normalize GrainTrade UA spot market data.

        Args:
            data: Raw GrainTrade UA records
            fx_rates: Current FX rates

        Returns:
            Normalized records
        """
        # Update FX rates from pipeline
        if "USD_UAH" in fx_rates:
            self.set_fx_rate("USD", "UAH", fx_rates["USD_UAH"])

        normalized = []

        # Commodity name mappings (Ukrainian to English)
        commodity_mappings = {
            "пшениця": "Wheat",
            "кукурудза": "Corn",
            "соняшник": "Sunflower",
            "соя": "Soybeans",
            "ячмінь": "Barley",
            "жито": "Rye",
            "овес": "Oats",
        }

        for record in data:
            # Extract commodity name and grade
            crop_raw = record.get("crop", "").lower()
            commodity_name = "Unknown"
            grade = None

            for ukr_name, eng_name in commodity_mappings.items():
                if ukr_name in crop_raw:
                    commodity_name = eng_name
                    # Extract grade if present
                    if "клас" in crop_raw:
                        grade_match = re.search(r"(\d+)\s*клас", crop_raw)
                        if grade_match:
                            grade = f"{grade_match.group(1)}nd Grade"
                    break

            # Convert price to USD
            price_uah = record.get("price_value", 0)
            price_usd = (
                price_uah * self._get_currency_rate("UAH", "USD") if price_uah else 0
            )

            # Parse offer type
            offer_type_raw = record.get("offer_type", "")
            offer_type = (
                "SELL"
                if "ПРОДАМ" in offer_type_raw
                else "BUY"
                if "КУПЛЮ" in offer_type_raw
                else "UNKNOWN"
            )

            normalized_record = {
                "record_id": f"graintrade_{record.get('_staging_id', 'unknown')}",
                "source": "graintradecomua",
                "commodity_name": commodity_name,
                "grade": grade,
                "price_usd": round(price_usd, 2),
                "price_uah": price_uah,
                "volume": float(record.get("volume_tons", 0) or 0),
                "price_type": "spot_offer",
                "currency": "UAH",
                "unit": "metric_ton",
                "delivery_term": record.get("delivery_term", ""),
                "region": record.get("basis", ""),
                "company": record.get("company", ""),
                "offer_type": offer_type,
                "source_timestamp": record.get("offer_date"),
                "processed_at": datetime.now().isoformat(),
                "raw_data": record,
            }
            normalized.append(normalized_record)

        self.normalization_log.append(
            {
                "operation": "normalize_graintradecomua_data",
                "records_processed": len(data),
                "output_records": len(normalized),
                "usd_uah_rate": fx_rates.get("USD_UAH", "not_provided"),
            }
        )

        return normalized

    def normalize_tripoli_land_data(
        self, data: List[Dict], fx_rates: Dict[str, float]
    ) -> List[Dict]:
        """
        Normalize Tripoli Land storage price data.

        Args:
            data: Raw Tripoli Land records
            fx_rates: Current FX rates

        Returns:
            Normalized records
        """
        # Update FX rates
        if "USD_UAH" in fx_rates:
            self.set_fx_rate("USD", "UAH", fx_rates["USD_UAH"])

        normalized = []

        # Storage type mappings
        storage_mappings = {
            "порт": "Port Storage",
            "елеватор": "Elevator",
            "склад": "Warehouse",
        }

        for record in data:
            # Map Ukrainian commodity names
            culture_ua = record.get("culture_ua", "").lower()
            commodity_name = record.get("category_name", "Unknown")

            # Ukrainian to English mapping for consistency
            ukr_to_eng = {
                "пшениця": "Wheat",
                "кукурудза": "Corn",
                "соняшник": "Sunflower",
                "соя": "Soybeans",
                "ячмінь": "Barley",
            }

            for ukr, eng in ukr_to_eng.items():
                if ukr in culture_ua:
                    commodity_name = eng
                    break

            # Extract grade
            grade = None
            if "клас" in culture_ua:
                grade_match = re.search(r"(\d+)\s*клас", culture_ua)
                if grade_match:
                    grade_num = int(grade_match.group(1))
                    grade = f"{grade_num}{'st' if grade_num == 1 else 'nd' if grade_num == 2 else 'rd' if grade_num == 3 else 'th'} Grade"

            # Convert price to USD
            price_uah = record.get("price_uah_per_ton", 0)
            price_usd = (
                price_uah * self._get_currency_rate("UAH", "USD") if price_uah else 0
            )

            # Storage type
            storage_type_ua = record.get("storage_type", "").lower()
            storage_type = storage_mappings.get(
                storage_type_ua, record.get("storage_type", "Unknown")
            )

            normalized_record = {
                "record_id": f"tripoli_{record.get('_staging_id', 'unknown')}",
                "source": "tripoli_land",
                "commodity_name": commodity_name,
                "grade": grade,
                "price_usd": round(price_usd, 2),
                "price_uah": price_uah,
                "volume": None,  # Storage prices don't have volume
                "price_type": "storage_rate",
                "currency": "UAH",
                "unit": "metric_ton",
                "market": storage_type,
                "region": record.get("address_region", ""),
                "company": record.get("company_name", ""),
                "facility": record.get("storage_location", ""),
                "source_timestamp": record.get("extracted_at"),
                "processed_at": datetime.now().isoformat(),
                "raw_data": record,
            }
            normalized.append(normalized_record)

        self.normalization_log.append(
            {
                "operation": "normalize_tripoli_land_data",
                "records_processed": len(data),
                "output_records": len(normalized),
                "usd_uah_rate": fx_rates.get("USD_UAH", "not_provided"),
            }
        )

        return normalized

    def normalize_currency_data(self, data: List[Dict]) -> Dict[str, float]:
        """
        Extract FX rates from currency data for use in other normalizations.

        Args:
            data: Currency records from NBU

        Returns:
            Dict of FX rates keyed by "BASE_QUOTE"
        """
        fx_rates = {}

        for record in data:
            if record.get("provider", "").lower() == "nbu":
                base = record.get("base_currency")
                quote = record.get("quote_currency")
                rate = record.get("rate")

                if base and quote and rate:
                    key = f"{base}_{quote}"
                    fx_rates[key] = rate

                    # Also store inverse
                    if rate > 0:
                        fx_rates[f"{quote}_{base}"] = 1 / rate

        # Ensure USD_UAH is available
        if "USD_UAH" not in fx_rates and "UAH_USD" in fx_rates:
            fx_rates["USD_UAH"] = 1 / fx_rates["UAH_USD"]

        self.normalization_log.append(
            {
                "operation": "normalize_currency_data",
                "fx_rates_extracted": len(fx_rates),
                "usd_uah_rate": fx_rates.get("USD_UAH", "not_found"),
            }
        )

        return fx_rates

    def get_normalization_report(self) -> Dict[str, Any]:
        """
        Get a report of all normalization operations performed.

        Returns:
            Dictionary with normalization operation logs
        """
        return {
            "operations": len(self.normalization_log),
            "operations_detail": self.normalization_log,
            "total_records_processed": sum(
                log.get("total_records", 0) for log in self.normalization_log
            ),
        }

        self.normalization_log.append(
            {
                "operation": "round_numeric_values",
                "columns": numeric_columns,
                "decimals": decimals,
                "rows_processed": len(data),
            }
        )

        return normalized

    def get_normalization_report(self) -> Dict[str, Any]:
        """Get summary of all normalization operations."""
        return {
            "operations_count": len(self.normalization_log),
            "operations": self.normalization_log,
        }
