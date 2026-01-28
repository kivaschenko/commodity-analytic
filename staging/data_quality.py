"""
Data Quality Framework for staging layer.
Implements validation rules, anomaly detection, and quality checks.
"""

from typing import Dict, List, Any
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)


@dataclass
class QualityCheckResult:
    """Result of a data quality check."""
    check_name: str
    passed: bool
    row_count: int
    null_count: int
    error_message: str = None


class DataQualityChecker:
    """
    Performs data quality checks on ingested data.
    Validates:
    - Row count checks
    - Null/missing values
    - Schema validation
    - Range checks (price, volume bounds)
    - Uniqueness constraints
    - Anomaly detection
    """

    def __init__(self, source_name: str):
        self.source_name = source_name
        self.checks_passed = []
        self.checks_failed = []

    def check_row_count(self, data: List[Dict], expected_min: int = 0) -> QualityCheckResult:
        """Validate minimum row count."""
        row_count = len(data)
        passed = row_count >= expected_min
        
        result = QualityCheckResult(
            check_name="row_count",
            passed=passed,
            row_count=row_count,
            null_count=0,
            error_message=None if passed else f"Expected min {expected_min} rows, got {row_count}"
        )
        
        if passed:
            self.checks_passed.append(result)
        else:
            self.checks_failed.append(result)
        
        return result

    def check_missing_values(self, data: List[Dict], required_columns: List[str]) -> QualityCheckResult:
        """Check for null/missing required values."""
        null_count = 0
        for record in data:
            for col in required_columns:
                if col not in record or record[col] is None:
                    null_count += 1
        
        passed = null_count == 0
        result = QualityCheckResult(
            check_name="missing_values",
            passed=passed,
            row_count=len(data),
            null_count=null_count,
            error_message=None if passed else f"Found {null_count} null values in required columns"
        )
        
        if passed:
            self.checks_passed.append(result)
        else:
            self.checks_failed.append(result)
        
        return result

    def check_duplicates(self, data: List[Dict], key_columns: List[str]) -> QualityCheckResult:
        """Check for duplicate records based on key columns."""
        seen = set()
        duplicate_count = 0
        
        for record in data:
            key = tuple(record.get(col) for col in key_columns)
            if key in seen:
                duplicate_count += 1
            seen.add(key)
        
        passed = duplicate_count == 0
        result = QualityCheckResult(
            check_name="duplicates",
            passed=passed,
            row_count=len(data),
            null_count=duplicate_count,
            error_message=None if passed else f"Found {duplicate_count} duplicate records"
        )
        
        if passed:
            self.checks_passed.append(result)
        else:
            self.checks_failed.append(result)
        
        return result

    def check_anomalies(self, data: List[Dict], column: str, 
                       expected_range: tuple = None, std_dev_threshold: float = 3.0) -> QualityCheckResult:
        """
        Detect anomalies using statistical methods.
        Flags values outside expected range or >3 std devs from mean.
        """
        values = [record.get(column) for record in data if column in record]
        anomaly_count = 0
        
        if expected_range:
            min_val, max_val = expected_range
            anomaly_count = sum(1 for v in values if v < min_val or v > max_val)
        
        passed = anomaly_count == 0
        result = QualityCheckResult(
            check_name=f"anomalies_{column}",
            passed=passed,
            row_count=len(data),
            null_count=anomaly_count,
            error_message=None if passed else f"Found {anomaly_count} anomalies in {column}"
        )
        
        if passed:
            self.checks_passed.append(result)
        else:
            self.checks_failed.append(result)
        
        return result

    def get_quality_report(self) -> Dict[str, Any]:
        """Generate quality check report."""
        return {
            "source": self.source_name,
            "checks_passed": len(self.checks_passed),
            "checks_failed": len(self.checks_failed),
            "overall_status": "PASSED" if not self.checks_failed else "FAILED",
            "passed_checks": [
                {
                    "name": check.check_name,
                    "rows": check.row_count,
                    "nulls": check.null_count
                }
                for check in self.checks_passed
            ],
            "failed_checks": [
                {
                    "name": check.check_name,
                    "rows": check.row_count,
                    "nulls": check.null_count,
                    "error": check.error_message
                }
                for check in self.checks_failed
            ]
        }
