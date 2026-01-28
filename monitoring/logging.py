"""
Pipeline Logging - Structured logging for pipeline operations.
"""

import logging
import json
from typing import Dict, Any, Optional
from datetime import datetime
from enum import Enum


class LogLevel(Enum):
    """Log levels."""
    DEBUG = logging.DEBUG
    INFO = logging.INFO
    WARNING = logging.WARNING
    ERROR = logging.ERROR
    CRITICAL = logging.CRITICAL


class PipelineLogger:
    """
    Structured logging for pipeline operations.
    Logs to file and console with structured JSON format for analysis.
    """

    def __init__(self, logger_name: str = "commodity_pipeline",
                 log_file: str = None):
        """
        Args:
            logger_name: Logger name
            log_file: Path to log file
        """
        self.logger = logging.getLogger(logger_name)
        self.logger.setLevel(logging.DEBUG)

        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        console_handler.setFormatter(console_formatter)
        self.logger.addHandler(console_handler)

        # File handler (if provided)
        if log_file:
            file_handler = logging.FileHandler(log_file)
            file_handler.setLevel(logging.DEBUG)
            file_formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            file_handler.setFormatter(file_formatter)
            self.logger.addHandler(file_handler)

        self.operation_log = []

    def log_operation(self, operation_name: str,
                     status: str,
                     details: Dict[str, Any] = None,
                     level: LogLevel = LogLevel.INFO) -> None:
        """
        Log a pipeline operation.
        
        Args:
            operation_name: Name of operation
            status: Status (started, completed, failed)
            details: Operation details
            level: Log level
        """
        log_entry = {
            "timestamp": datetime.utcnow().isoformat(),
            "operation": operation_name,
            "status": status,
            "details": details or {},
        }

        self.operation_log.append(log_entry)

        # Log as JSON for easy parsing
        json_log = json.dumps(log_entry)
        self.logger.log(level.value, json_log)

    def log_extraction(self, source: str,
                      record_count: int,
                      duration_seconds: float,
                      status: str = "completed") -> None:
        """Log data extraction operation."""
        self.log_operation(
            operation_name=f"extract_{source}",
            status=status,
            details={
                "source": source,
                "record_count": record_count,
                "duration_seconds": duration_seconds
            }
        )

    def log_transformation(self, operation_type: str,
                          input_count: int,
                          output_count: int,
                          duration_seconds: float,
                          status: str = "completed") -> None:
        """Log data transformation operation."""
        self.log_operation(
            operation_name=f"transform_{operation_type}",
            status=status,
            details={
                "operation_type": operation_type,
                "input_count": input_count,
                "output_count": output_count,
                "duration_seconds": duration_seconds,
                "records_dropped": input_count - output_count
            }
        )

    def log_quality_check(self, check_name: str,
                         passed: bool,
                         details: Dict[str, Any] = None) -> None:
        """Log quality check result."""
        level = LogLevel.INFO if passed else LogLevel.ERROR
        self.log_operation(
            operation_name=f"quality_check_{check_name}",
            status="passed" if passed else "failed",
            details=details or {},
            level=level
        )

    def log_warehouse_load(self, table_name: str,
                          records_inserted: int,
                          records_updated: int,
                          duration_seconds: float,
                          status: str = "completed") -> None:
        """Log warehouse load operation."""
        self.log_operation(
            operation_name=f"warehouse_load_{table_name}",
            status=status,
            details={
                "table": table_name,
                "records_inserted": records_inserted,
                "records_updated": records_updated,
                "duration_seconds": duration_seconds
            }
        )

    def log_error(self, operation: str,
                 error_message: str,
                 traceback: str = None) -> None:
        """Log error with full details."""
        self.log_operation(
            operation_name=operation,
            status="failed",
            details={
                "error_message": error_message,
                "traceback": traceback
            },
            level=LogLevel.ERROR
        )

    def log_warning(self, operation: str,
                   warning_message: str,
                   details: Dict[str, Any] = None) -> None:
        """Log warning."""
        self.log_operation(
            operation_name=operation,
            status="warning",
            details={
                "warning_message": warning_message,
                **(details or {})
            },
            level=LogLevel.WARNING
        )

    def get_operation_log(self, limit: int = 100) -> list:
        """Get recent operations."""
        return self.operation_log[-limit:]

    def get_operation_summary(self) -> Dict[str, Any]:
        """Get summary of logged operations."""
        status_counts = {}
        operation_counts = {}

        for entry in self.operation_log:
            status = entry.get("status")
            operation = entry.get("operation")

            status_counts[status] = status_counts.get(status, 0) + 1
            operation_counts[operation] = operation_counts.get(operation, 0) + 1

        return {
            "total_operations": len(self.operation_log),
            "status_distribution": status_counts,
            "operations_distribution": operation_counts,
            "timestamp": datetime.utcnow().isoformat()
        }
