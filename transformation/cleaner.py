"""
Data Cleaner - Handles data cleaning operations.
Removes duplicates, handles missing values, standardizes formats.
"""

import logging
from typing import List, Dict, Any, Optional
import re
from datetime import datetime

logger = logging.getLogger(__name__)


class DataCleaner:
    """
    Cleans and sanitizes raw commodity data:
    - Remove duplicates
    - Handle missing values
    - Standardize data formats
    - Outlier detection and treatment
    - Type conversions
    """

    def __init__(self, commodity_type: str = "grain"):
        self.commodity_type = commodity_type
        self.cleaning_log = []

    def remove_duplicates(self, data: List[Dict], 
                         key_columns: List[str]) -> List[Dict]:
        """
        Remove duplicate records based on key columns.
        
        Args:
            data: Raw records
            key_columns: Columns that form unique key
        
        Returns:
            Deduplicated data
        """
        seen = set()
        cleaned = []
        duplicates_removed = 0

        for record in data:
            key = tuple(record.get(col) for col in key_columns)
            if key not in seen:
                cleaned.append(record)
                seen.add(key)
            else:
                duplicates_removed += 1

        self.cleaning_log.append({
            "operation": "remove_duplicates",
            "rows_removed": duplicates_removed,
            "rows_remaining": len(cleaned)
        })

        logger.info(f"Removed {duplicates_removed} duplicates, {len(cleaned)} records remaining")
        return cleaned

    def handle_missing_values(self, data: List[Dict],
                             strategy: str = "drop",
                             fill_value: Any = None) -> List[Dict]:
        """
        Handle missing/null values in records.
        
        Args:
            data: Records with potential missing values
            strategy: "drop" (remove rows), "fill" (use fill_value), "forward_fill"
            fill_value: Value to use for "fill" strategy
        
        Returns:
            Cleaned data
        """
        cleaned = []
        rows_dropped = 0

        for record in data:
            if strategy == "drop":
                # Drop records with any null values
                if all(v is not None for v in record.values()):
                    cleaned.append(record)
                else:
                    rows_dropped += 1
            elif strategy == "fill":
                # Fill null values with provided value
                filled_record = {
                    k: fill_value if v is None else v 
                    for k, v in record.items()
                }
                cleaned.append(filled_record)

        self.cleaning_log.append({
            "operation": "handle_missing_values",
            "strategy": strategy,
            "rows_affected": rows_dropped if strategy == "drop" else len(data),
            "rows_remaining": len(cleaned)
        })

        return cleaned

    def standardize_commodity_names(self, data: List[Dict],
                                   commodity_column: str = "commodity",
                                   mapping: Dict[str, str] = None) -> List[Dict]:
        """
        Standardize commodity names across sources.
        
        Args:
            data: Records to clean
            commodity_column: Column containing commodity names
            mapping: Dict mapping non-standard names to standard names
        
        Returns:
            Data with standardized names
        """
        if mapping is None:
            mapping = {
                "wheat": "Wheat",
                "corn": "Corn",
                "soybeans": "Soybeans",
                "barley": "Barley",
                "rapeseed": "Rapeseed",
                "usd": "USD",
                "eur": "EUR",
                "uah": "UAH",
            }

        cleaned = []
        standardized_count = 0

        for record in data:
            cleaned_record = record.copy()
            if commodity_column in record:
                original = str(record[commodity_column]).lower().strip()
                
                # Exact match
                if original in mapping:
                    cleaned_record[commodity_column] = mapping[original]
                    standardized_count += 1
                # Fuzzy match
                elif any(original in std_name.lower() 
                        for std_name in mapping.values()):
                    for non_std, std in mapping.items():
                        if original in std.lower():
                            cleaned_record[commodity_column] = std
                            standardized_count += 1
                            break

            cleaned.append(cleaned_record)

        self.cleaning_log.append({
            "operation": "standardize_commodity_names",
            "rows_standardized": standardized_count,
            "total_rows": len(data)
        })

        return cleaned

    def detect_outliers(self, data: List[Dict],
                       column: str,
                       method: str = "iqr",
                       threshold: float = 1.5) -> tuple[List[Dict], List[int]]:
        """
        Detect outliers in numeric columns.
        
        Args:
            data: Records
            column: Column to analyze
            method: "iqr" (interquartile range) or "zscore"
            threshold: IQR multiplier or z-score threshold
        
        Returns:
            (cleaned_data, outlier_indices)
        """
        values = [record[column] for record in data 
                 if column in record and isinstance(record[column], (int, float))]
        
        if not values:
            return data, []

        outlier_indices = []

        if method == "iqr":
            sorted_vals = sorted(values)
            q1 = sorted_vals[len(sorted_vals)//4]
            q3 = sorted_vals[3*len(sorted_vals)//4]
            iqr = q3 - q1
            lower_bound = q1 - threshold * iqr
            upper_bound = q3 + threshold * iqr

            for idx, record in enumerate(data):
                if column in record:
                    val = record[column]
                    if isinstance(val, (int, float)):
                        if val < lower_bound or val > upper_bound:
                            outlier_indices.append(idx)

        cleaned = [record for idx, record in enumerate(data) 
                  if idx not in outlier_indices]

        self.cleaning_log.append({
            "operation": "detect_outliers",
            "column": column,
            "method": method,
            "outliers_found": len(outlier_indices),
            "rows_remaining": len(cleaned)
        })

        return cleaned, outlier_indices

    def get_cleaning_report(self) -> Dict[str, Any]:
        """Get summary of all cleaning operations performed."""
        return {
            "commodity_type": self.commodity_type,
            "operations_count": len(self.cleaning_log),
            "operations": self.cleaning_log
        }
