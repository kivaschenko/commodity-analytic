"""
Schema and data type validators for staging layer.
"""

from typing import Dict, List, Any, Union
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class DataType(Enum):
    """Supported data types."""
    STRING = "string"
    INTEGER = "integer"
    FLOAT = "float"
    DECIMAL = "decimal"
    DATE = "date"
    DATETIME = "datetime"
    BOOLEAN = "boolean"


class SchemaValidator:
    """
    Validates data schema against expected structure.
    Checks column names, data types, and nullable constraints.
    """

    def __init__(self, schema: Dict[str, Dict]):
        """
        Args:
            schema: Dict mapping column_name -> {'type': DataType, 'nullable': bool}
        """
        self.schema = schema

    def validate(self, data: List[Dict]) -> tuple[bool, List[str]]:
        """
        Validate data against schema.
        
        Returns:
            (is_valid, error_messages)
        """
        errors = []

        if not data:
            errors.append("Data is empty")
            return False, errors

        # Check columns exist
        first_record = data[0]
        for col_name in self.schema.keys():
            if col_name not in first_record:
                errors.append(f"Missing required column: {col_name}")

        # Check column types and nullability
        for record in data:
            for col_name, col_spec in self.schema.items():
                if col_name not in record:
                    if not col_spec.get("nullable", False):
                        errors.append(f"Non-nullable column {col_name} is missing")
                elif record[col_name] is None:
                    if not col_spec.get("nullable", False):
                        errors.append(f"Non-nullable column {col_name} is null")

        return len(errors) == 0, errors


class RangeValidator:
    """
    Validates that numeric values fall within expected ranges.
    Used for price, volume, and other numeric commodity data.
    """

    def __init__(self, validation_rules: Dict[str, Dict[str, float]]):
        """
        Args:
            validation_rules: Dict mapping column_name -> {'min': float, 'max': float}
        """
        self.rules = validation_rules

    def validate(self, data: List[Dict]) -> tuple[bool, List[Dict]]:
        """
        Validate numeric ranges.
        
        Returns:
            (is_valid, out_of_range_records)
        """
        out_of_range = []

        for idx, record in enumerate(data):
            for col_name, limits in self.rules.items():
                if col_name not in record:
                    continue

                value = record[col_name]
                if value is None:
                    continue

                min_val = limits.get("min")
                max_val = limits.get("max")

                if min_val is not None and value < min_val:
                    out_of_range.append({
                        "row_index": idx,
                        "column": col_name,
                        "value": value,
                        "min_allowed": min_val,
                        "issue": "below_minimum"
                    })

                if max_val is not None and value > max_val:
                    out_of_range.append({
                        "row_index": idx,
                        "column": col_name,
                        "value": value,
                        "max_allowed": max_val,
                        "issue": "above_maximum"
                    })

        return len(out_of_range) == 0, out_of_range
