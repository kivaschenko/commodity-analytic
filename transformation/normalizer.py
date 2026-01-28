"""
Data Normalizer - Standardizes data formats and units.
Normalizes prices, units, currencies, and timestamps.
"""

import logging
from typing import List, Dict, Any
from datetime import datetime

logger = logging.getLogger(__name__)


class DataNormalizer:
    """
    Normalizes commodity data to standard formats:
    - Price normalization (convert all to USD/standard unit)
    - Unit standardization (tons, bushels, etc.)
    - Currency conversion
    - Timestamp standardization
    - Numeric precision
    """

    def __init__(self):
        self.normalization_log = []
        # Exchange rates (simplified - should come from external service)
        self.exchange_rates = {
            "USD": 1.0,
            "EUR": 1.10,  # EUR to USD
            "UAH": 0.027,  # UAH to USD
        }
        # Unit conversions
        self.unit_conversions = {
            "ton": 1.0,
            "tonne": 1.0,
            "bushel": 0.0272155,  # bushel to ton
            "kg": 0.001,
            "lb": 0.000453592,
        }

    def normalize_prices(self, data: List[Dict],
                        price_column: str = "price",
                        currency_column: str = "currency",
                        base_currency: str = "USD") -> List[Dict]:
        """
        Convert all prices to base currency.
        
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

        for record in normalized:
            normalized_record = record.copy()
            
            if price_column in record and currency_column in record:
                original_price = record[price_column]
                original_currency = record[currency_column]

                if original_currency != base_currency:
                    rate = self.exchange_rates.get(original_currency, 1.0)
                    normalized_record[price_column] = original_price * rate
                    normalized_record[f"{price_column}_base_currency"] = base_currency
                    conversions_count += 1

            normalized.append(normalized_record)

        self.normalization_log.append({
            "operation": "normalize_prices",
            "base_currency": base_currency,
            "conversions": conversions_count
        })

        return normalized

    def normalize_units(self, data: List[Dict],
                       quantity_column: str = "quantity",
                       unit_column: str = "unit",
                       target_unit: str = "ton") -> List[Dict]:
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
                original_unit = record[unit_column].lower()

                if original_unit != target_unit:
                    conversion_factor = self.unit_conversions.get(original_unit, 1.0)
                    normalized_record[quantity_column] = original_qty * conversion_factor
                    normalized_record[unit_column] = target_unit
                    conversions_count += 1

            normalized.append(normalized_record)

        self.normalization_log.append({
            "operation": "normalize_units",
            "target_unit": target_unit,
            "conversions": conversions_count
        })

        return normalized

    def normalize_timestamps(self, data: List[Dict],
                            timestamp_column: str = "date",
                            output_format: str = "%Y-%m-%d") -> List[Dict]:
        """
        Standardize all timestamps to ISO format.
        
        Args:
            data: Records with various timestamp formats
            timestamp_column: Column with timestamps
            output_format: Target format
        
        Returns:
            Data with normalized timestamps
        """
        normalized = []
        parse_errors = 0

        for record in data:
            normalized_record = record.copy()
            
            if timestamp_column in record:
                ts_value = record[timestamp_column]
                
                # Try parsing if it's a string
                if isinstance(ts_value, str):
                    try:
                        # Try common formats
                        for fmt in ["%Y-%m-%d", "%Y/%m/%d", "%d-%m-%Y", "%d.%m.%Y"]:
                            try:
                                parsed_ts = datetime.strptime(ts_value, fmt)
                                normalized_record[timestamp_column] = parsed_ts.strftime(output_format)
                                break
                            except ValueError:
                                continue
                        else:
                            parse_errors += 1
                    except Exception as e:
                        logger.warning(f"Failed to parse timestamp: {ts_value}")
                        parse_errors += 1
                elif isinstance(ts_value, datetime):
                    normalized_record[timestamp_column] = ts_value.strftime(output_format)

            normalized.append(normalized_record)

        self.normalization_log.append({
            "operation": "normalize_timestamps",
            "target_format": output_format,
            "parse_errors": parse_errors
        })

        return normalized

    def round_numeric_values(self, data: List[Dict],
                            numeric_columns: List[str],
                            decimals: int = 2) -> List[Dict]:
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

        self.normalization_log.append({
            "operation": "round_numeric_values",
            "columns": numeric_columns,
            "decimals": decimals,
            "rows_processed": len(data)
        })

        return normalized

    def get_normalization_report(self) -> Dict[str, Any]:
        """Get summary of all normalization operations."""
        return {
            "operations_count": len(self.normalization_log),
            "operations": self.normalization_log
        }
