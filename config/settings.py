"""
Configuration Management
"""

import os
import logging
from typing import Dict, Any
from pathlib import Path

from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__file__)
BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")
logger.info(f"Loaded environment variables from {BASE_DIR / '.env'}")


class Settings:
    """Application settings based on environment."""

    def __init__(self):
        self.log_level = (
            "INFO"  # Default log level, can be overridden by environment variables
        )
        self.env = os.getenv("ENVIRONMENT", "Undefined")
        self._load_settings()
        logger.info(f"Settings initialized for {self.env} environment")

    def _load_settings(self) -> None:
        """Common environment settings."""
        self.database_url = os.getenv("DATABASE_URL")
        self.data_lake_path = os.getenv("DATA_LAKE_PATH", "s3://data-lake-prod")
        self.warehouse_type = "snowflake"
        self.staging_enabled = True
        self.quality_checks_enabled = True
        self.slack_webhook = os.getenv("SLACK_WEBHOOK_URL")
        self.email_enabled = True
        self.log_level = "WARNING"
        self.cache_enabled = True
        self.storage_type = os.getenv("STORAGE_TYPE", "Undefined")
        self.storage_endpoint = os.getenv("STORAGE_ENDPOINT")
        self.storage_access_key = os.getenv("STORAGE_ACCESS_KEY")
        self.storage_secret_key = os.getenv("STORAGE_SECRET_KEY")
        self.storage_region = os.getenv("STORAGE_REGION", "us-east-1")
        self.bronze_bucket = os.getenv("STORAGE_BUCKET_BRONZE", "bronze-layer")
        self.silver_bucket = os.getenv("STORAGE_BUCKET_SILVER", "silver-layer")
        self.smtp_server = os.getenv("SMTP_SERVER")
        self.smtp_port = int(os.getenv("SMTP_PORT", 587))
        self.smtp_user = os.getenv("SMTP_USER")
        self.smtp_password = os.getenv("SMTP_PASSWORD")
        self.smtp_use_tls = os.getenv("SMTP_USE_TLS", "True").lower() in (
            "true",
            "1",
            "yes",
        )
        self.alert_email = os.getenv("ALERT_EMAIL")
        self.alert_recipients = [self.alert_email] if self.alert_email else []

    def get_all_settings(self) -> Dict[str, Any]:
        """Get all current settings."""
        settings_dict = {
            "environment": self.env,
            "database_url": self.database_url,
            "data_lake_path": self.data_lake_path,
            "warehouse_type": self.warehouse_type,
            "staging_enabled": self.staging_enabled,
            "quality_checks_enabled": self.quality_checks_enabled,
            "slack_webhook": self.slack_webhook,
            "email_enabled": self.email_enabled,
            "cache_enabled": self.cache_enabled,
            "storage_type": self.storage_type,
            "storage_endpoint": self.storage_endpoint,
            "storage_access_key": self.storage_access_key,
            "storage_secret_key": self.storage_secret_key,
            "storage_region": self.storage_region,
            "bronze_bucket": self.bronze_bucket,
            "silver_bucket": self.silver_bucket,
            "log_level": self.log_level,
            "smtp_server": self.smtp_server,
            "smtp_port": self.smtp_port,
            "smtp_user": self.smtp_user,
            "smtp_use_tls": self.smtp_use_tls,
            "alert_email": self.alert_email,
            "alert_recipients": self.alert_recipients,
        }
        return settings_dict


# Global settings instance
settings = Settings()

if __name__ == "__main__":
    # For testing purposes, print all settings
    all_settings = settings.get_all_settings()
    print("Current Application Settings:")
    for key, value in all_settings.items():
        print(f"{key}: {value}")
    print(f"type of settings: {type(settings)}")