"""
Configuration Management
"""

import os
import logging
from typing import Dict, Any
from pathlib import Path
from enum import Enum
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__file__)
BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")
logger.info(f"Loaded environment variables from {BASE_DIR / '.env'}")


class Environment(Enum):
    """Supported environments."""

    DEV = "development"
    STAGING = "staging"
    PROD = "production"


class Settings:
    """Application settings based on environment."""

    def __init__(self, env: str = "development"):
        self.env = Environment(env or os.getenv("ENVIRONMENT", "development").lower())
        self._load_settings()
        logger.info(f"Settings initialized for {self.env.value} environment")

    def _load_settings(self) -> None:
        """Load environment-specific settings."""
        if self.env == Environment.DEV:
            logger.info("Loading development environment settings")
            self._load_dev_settings()
        elif self.env == Environment.STAGING:
            logger.info("Loading staging environment settings")
            self._load_staging_settings()
        elif self.env == Environment.PROD:
            logger.info("Loading production environment settings")
            self._load_prod_settings()

    def _load_dev_settings(self) -> None:
        """Development environment settings."""
        self.database_url = os.getenv(
            "DB_URL",
            "postgresql+psycopg2://warehouse_user:teomeo2358@localhost:5432/commodity_warehouse",
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
        self.data_lake_path = os.getenv(
            "DATA_LAKE_PATH_STAGING", "s3://data-lake-staging"
        )
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
