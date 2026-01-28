"""
Configuration Management
"""

import os
from typing import Dict, Any
from enum import Enum


class Environment(Enum):
    """Supported environments."""
    DEV = "dev"
    STAGING = "staging"
    PROD = "prod"


class Settings:
    """Application settings based on environment."""

    def __init__(self, env: str = None):
        self.env = Environment(env or os.getenv("ENVIRONMENT", "dev"))
        self._load_settings()

    def _load_settings(self) -> None:
        """Load environment-specific settings."""
        if self.env == Environment.DEV:
            self._load_dev_settings()
        elif self.env == Environment.STAGING:
            self._load_staging_settings()
        elif self.env == Environment.PROD:
            self._load_prod_settings()

    def _load_dev_settings(self) -> None:
        """Development environment settings."""
        self.database_url = os.getenv(
            "DB_URL",
            "postgresql://airflow:airflow@localhost:5432/airflow"
        )
        self.data_lake_path = os.getenv("DATA_LAKE_PATH", "./data_lake")
        self.warehouse_type = "postgres"
        self.staging_enabled = True
        self.quality_checks_enabled = True
        self.slack_webhook = os.getenv("SLACK_WEBHOOK_URL")
        self.email_enabled = False
        self.log_level = "DEBUG"
        self.cache_enabled = False

    def _load_staging_settings(self) -> None:
        """Staging environment settings."""
        self.database_url = os.getenv("DB_URL_STAGING")
        self.data_lake_path = os.getenv("DATA_LAKE_PATH_STAGING", "s3://data-lake-staging")
        self.warehouse_type = "snowflake"
        self.staging_enabled = True
        self.quality_checks_enabled = True
        self.slack_webhook = os.getenv("SLACK_WEBHOOK_URL")
        self.email_enabled = True
        self.log_level = "INFO"
        self.cache_enabled = True

    def _load_prod_settings(self) -> None:
        """Production environment settings."""
        self.database_url = os.getenv("DB_URL_PROD")
        self.data_lake_path = os.getenv("DATA_LAKE_PATH_PROD", "s3://data-lake-prod")
        self.warehouse_type = "snowflake"
        self.staging_enabled = True
        self.quality_checks_enabled = True
        self.slack_webhook = os.getenv("SLACK_WEBHOOK_URL")
        self.email_enabled = True
        self.log_level = "WARNING"
        self.cache_enabled = True

    def get_all_settings(self) -> Dict[str, Any]:
        """Get all current settings."""
        return {
            "environment": self.env.value,
            "database_url": self.database_url,
            "data_lake_path": self.data_lake_path,
            "warehouse_type": self.warehouse_type,
            "staging_enabled": self.staging_enabled,
            "quality_checks_enabled": self.quality_checks_enabled,
            "log_level": self.log_level,
        }


# Global settings instance
settings = Settings()
