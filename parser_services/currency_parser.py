"""Currency parser aligned with extraction -> bronze staging flow."""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import requests
from dotenv import load_dotenv
from pandas import DataFrame

from parser_services.base_parser import BaseParser
from staging.staging_handler import StagingHandler

logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")

CURRENCY_API_MAPPING: Dict[str, str] = {
    "NBU": "https://bank.gov.ua/NBUStatService/v1/statdirectory/exchange?json",
    "PrivatBank": "https://api.privatbank.ua/p24api/pubinfo?json&exchange&coursid=5",
    "ExchangeRateAPI": "https://api.exchangerate-api.com/v4/latest/USD",
}


class CurrencyParser(BaseParser):
    """Extracts FX rates from multiple providers and stages raw records to bronze."""

    def __init__(self, source_name: str = "currency", storage_type: str = "minio") -> None:
        self.source_name = source_name
        self.storage_type = storage_type
        self.bronze_bucket = os.getenv("MINIO_BUCKET_BRONZE", "bronze-layer")
        self.staging_handler = StagingHandler(
            staging_path=self.bronze_bucket,
            storage_type=self.storage_type,
        )

    def parse(self) -> List[Dict[str, Any]]:
        extracted_at = datetime.now(timezone.utc).isoformat()
        rows: List[Dict[str, Any]] = []

        for provider, api_url in CURRENCY_API_MAPPING.items():
            payload = self._fetch_payload(provider=provider, api_url=api_url)
            if payload is None:
                rows.append(
                    {
                        "provider": provider,
                        "source_endpoint": api_url,
                        "base_currency": None,
                        "quote_currency": None,
                        "rate": None,
                        "buy_rate": None,
                        "sell_rate": None,
                        "rate_type": None,
                        "source_timestamp": None,
                        "note": "provider_unavailable",
                        "extracted_at": extracted_at,
                    }
                )
                continue

            provider_rows = self._normalize_provider_payload(
                provider=provider,
                payload=payload,
                source_endpoint=api_url,
                extracted_at=extracted_at,
            )

            if not provider_rows:
                rows.append(
                    {
                        "provider": provider,
                        "source_endpoint": api_url,
                        "base_currency": None,
                        "quote_currency": None,
                        "rate": None,
                        "buy_rate": None,
                        "sell_rate": None,
                        "rate_type": None,
                        "source_timestamp": None,
                        "note": "no_rows_after_normalization",
                        "extracted_at": extracted_at,
                    }
                )
            else:
                rows.extend(provider_rows)

        logger.info("Fetched %s currency records", len(rows))
        return rows

    def save_results(
        self,
        results: "List[Dict[str, Any]] | Any | DataFrame",
        filepath: str = "",
        file_ext: str = "json",
        storage_type: str = "minio",
    ) -> None:
        if isinstance(results, DataFrame):
            records = results.to_dict("records")
        elif isinstance(results, list):
            records = results
        else:
            logger.warning("Unsupported result type: %s", type(results))
            return

        if not records:
            logger.warning("No currency records to stage")
            return

        handler = self.staging_handler
        if storage_type != self.storage_type:
            handler = StagingHandler(
                staging_path=self.bronze_bucket,
                storage_type=storage_type,
            )

        enriched = handler.add_staging_metadata(records, source_name=self.source_name)
        staged_path = handler.stage_raw_data(
            data=enriched,
            source_name=self.source_name,
            file_format=file_ext,
        )
        logger.info("Currency raw payload staged to %s", staged_path)

    def _stage_records(
        self,
        records: List[Dict[str, Any]],
        storage_type: str,
        file_ext: str = "json",
    ) -> str:
        handler = self.staging_handler
        if storage_type != self.storage_type:
            handler = StagingHandler(
                staging_path=self.bronze_bucket,
                storage_type=storage_type,
            )

        enriched = handler.add_staging_metadata(records, source_name=self.source_name)
        staged_path = handler.stage_raw_data(
            data=enriched,
            source_name=self.source_name,
            file_format=file_ext,
        )
        logger.info("Currency raw payload staged to %s", staged_path)
        return staged_path

    def parse_and_stage(self, storage_type: str | None = None) -> Dict[str, Any]:
        records = self.parse()
        effective_storage = storage_type or self.storage_type
        if not records:
            return {
                "status": "no_data",
                "source": self.source_name,
                "record_count": 0,
                "storage_type": effective_storage,
                "staged_path": None,
            }

        staged_path = self._stage_records(records, storage_type=effective_storage, file_ext="json")

        return {
            "status": "success",
            "source": self.source_name,
            "record_count": len(records),
            "storage_type": effective_storage,
            "staged_path": staged_path,
        }

    @staticmethod
    def _fetch_payload(provider: str, api_url: str) -> Any | None:
        try:
            logger.info("Fetching currency data from %s", provider)
            response = requests.get(api_url, timeout=15)
            response.raise_for_status()
            return response.json()
        except Exception as exc:  # noqa: BLE001
            logger.error("Currency fetch failed for %s: %s", provider, exc)
            return None

    def _normalize_provider_payload(
        self,
        provider: str,
        payload: Any,
        source_endpoint: str,
        extracted_at: str,
    ) -> List[Dict[str, Any]]:
        if provider == "NBU":
            return self._normalize_nbu(payload, source_endpoint, extracted_at)
        if provider == "PrivatBank":
            return self._normalize_privat(payload, source_endpoint, extracted_at)
        if provider == "ExchangeRateAPI":
            return self._normalize_exchangerate_api(payload, source_endpoint, extracted_at)

        return [
            {
                "provider": provider,
                "source_endpoint": source_endpoint,
                "base_currency": None,
                "quote_currency": None,
                "rate": None,
                "buy_rate": None,
                "sell_rate": None,
                "rate_type": None,
                "source_timestamp": None,
                "note": "unsupported_provider",
                "extracted_at": extracted_at,
            }
        ]

    @staticmethod
    def _normalize_nbu(payload: Any, source_endpoint: str, extracted_at: str) -> List[Dict[str, Any]]:
        if not isinstance(payload, list):
            return []

        rows: List[Dict[str, Any]] = []
        for item in payload:
            if not isinstance(item, dict):
                continue

            quote = item.get("cc")
            rate = item.get("rate")
            if quote is None or rate is None:
                continue

            rows.append(
                {
                    "provider": "NBU",
                    "source_endpoint": source_endpoint,
                    "base_currency": str(quote),
                    "quote_currency": "UAH",
                    "rate": float(rate),
                    "buy_rate": None,
                    "sell_rate": None,
                    "rate_type": "official",
                    "source_timestamp": item.get("exchangedate"),
                    "note": "ok",
                    "extracted_at": extracted_at,
                }
            )

        return rows

    @staticmethod
    def _normalize_privat(payload: Any, source_endpoint: str, extracted_at: str) -> List[Dict[str, Any]]:
        if not isinstance(payload, list):
            return []

        rows: List[Dict[str, Any]] = []
        for item in payload:
            if not isinstance(item, dict):
                continue

            base = item.get("ccy")
            quote = item.get("base_ccy")
            buy_rate = CurrencyParser._to_float(item.get("buy"))
            sell_rate = CurrencyParser._to_float(item.get("sale"))
            if not base or not quote or (buy_rate is None and sell_rate is None):
                continue

            mid_rate = None
            if buy_rate is not None and sell_rate is not None:
                mid_rate = (buy_rate + sell_rate) / 2

            rows.append(
                {
                    "provider": "PrivatBank",
                    "source_endpoint": source_endpoint,
                    "base_currency": str(base),
                    "quote_currency": str(quote),
                    "rate": mid_rate,
                    "buy_rate": buy_rate,
                    "sell_rate": sell_rate,
                    "rate_type": "cash_market",
                    "source_timestamp": None,
                    "note": "ok",
                    "extracted_at": extracted_at,
                }
            )

        return rows

    @staticmethod
    def _normalize_exchangerate_api(
        payload: Any,
        source_endpoint: str,
        extracted_at: str,
    ) -> List[Dict[str, Any]]:
        if not isinstance(payload, dict):
            return []

        rates = payload.get("rates")
        base_currency = payload.get("base", "USD")
        source_date = payload.get("date")

        if not isinstance(rates, dict):
            return []

        rows: List[Dict[str, Any]] = []
        for quote_currency, value in rates.items():
            rate = CurrencyParser._to_float(value)
            if rate is None:
                continue

            rows.append(
                {
                    "provider": "ExchangeRateAPI",
                    "source_endpoint": source_endpoint,
                    "base_currency": str(base_currency),
                    "quote_currency": str(quote_currency),
                    "rate": rate,
                    "buy_rate": None,
                    "sell_rate": None,
                    "rate_type": "daily_reference",
                    "source_timestamp": source_date,
                    "note": "ok",
                    "extracted_at": extracted_at,
                }
            )

        return rows

    @staticmethod
    def _to_float(value: Any) -> float | None:
        try:
            if value is None:
                return None
            return float(value)
        except (TypeError, ValueError):
            return None


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    parser = CurrencyParser(storage_type="minio")
    result = parser.parse_and_stage(storage_type="minio")
    logger.info("Execution result: %s", result)
