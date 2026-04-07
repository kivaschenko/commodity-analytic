"""APK Inform parser aligned with extraction -> bronze staging flow."""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from pandas import DataFrame

from parser_services.base_parser import BaseParser
from staging.staging_handler import StagingHandler

logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")


class APKInformParser(BaseParser):
    """Extracts APK Inform grain market prices and stages raw records to bronze."""

    def __init__(
        self,
        source_name: str = "apkinform",
        storage_type: str = "minio",
        regions: List[str] | None = None,
    ) -> None:
        self.source_name = source_name
        self.storage_type = storage_type
        self.regions = regions or []
        self.base_url = "https://www.apk-inform.com/en/prices"
        self.headers = {
            "User-Agent": (
                "Mozilla/5.0 (X11; Linux x86_64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
            )
        }
        self.bronze_bucket = os.getenv("MINIO_BUCKET_BRONZE", "bronze-layer")
        self.staging_handler = StagingHandler(
            staging_path=self.bronze_bucket,
            storage_type=self.storage_type,
        )
        self.commodity_map = {
            "Wheat 3rd class": "Wheat",
            "Wheat 4th class": "Wheat",
            "Wheat 5th class": "Wheat",
            "Corn": "Corn",
            "Barley": "Barley",
            "Sunflower": "Sunflower Seeds",
            "Soybean": "Soybeans",
            "Rapeseed": "Rapeseed",
        }

    def parse(self) -> List[Dict[str, Any]]:
        extracted_at = datetime.now(timezone.utc).isoformat()
        rows: List[Dict[str, Any]] = []

        api_rows = self._fetch_from_api(extracted_at)
        if api_rows:
            rows.extend(api_rows)
        else:
            rows.extend(self._scrape_prices(extracted_at))

        if not rows:
            logger.warning("No data parsed from APK Inform")
            return []

        rows = self._normalize_rows(rows)
        logger.info("Fetched %s APK Inform records", len(rows))
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
            logger.warning("No APK Inform records to stage")
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
        logger.info("APK Inform raw payload staged to %s", staged_path)

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
        logger.info("APK Inform raw payload staged to %s", staged_path)
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

    def _fetch_from_api(self, extracted_at: str) -> List[Dict[str, Any]]:
        api_url = f"{self.base_url}/api/v1/daily-prices"
        try:
            response = requests.get(api_url, headers=self.headers, timeout=12)
            if response.status_code != 200:
                return []
            payload = response.json()
            return self._parse_api_response(payload, extracted_at)
        except Exception as exc:  # noqa: BLE001
            logger.info("APK Inform API endpoint unavailable, fallback to scraping: %s", exc)
            return []

    def _parse_api_response(self, payload: Any, extracted_at: str) -> List[Dict[str, Any]]:
        if not isinstance(payload, dict):
            return []

        rows: List[Dict[str, Any]] = []
        for item in payload.get("prices", []):
            if not isinstance(item, dict):
                continue

            region = self._normalize_region(item.get("region"))
            if self.regions and region not in self.regions:
                continue

            price = self._to_float(item.get("price"))
            rows.append(
                {
                    "commodity": str(item.get("commodity_name") or "").strip() or None,
                    "commodity_group": None,
                    "region": region,
                    "price": price,
                    "currency": str(item.get("currency") or "UAH").strip(),
                    "unit": str(item.get("unit") or "ton").strip(),
                    "quality": str(item.get("quality") or "").strip() or None,
                    "price_date": self._normalize_date(item.get("date")),
                    "source_endpoint": api_url_from_base(self.base_url),
                    "source_channel": "api",
                    "note": "ok" if price is not None else "invalid_price",
                    "extracted_at": extracted_at,
                }
            )

        return rows

    def _scrape_prices(self, extracted_at: str) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        source_endpoint = f"{self.base_url}/grains"

        try:
            response = requests.get(source_endpoint, headers=self.headers, timeout=15)
            if response.status_code != 200:
                logger.error("Failed to fetch APK Inform page: %s", response.status_code)
                return []

            soup = BeautifulSoup(response.text, "html.parser")
            table = soup.find("table", {"class": "prices-table"})
            if not table:
                logger.warning("Could not find price table on APK Inform page")
                return []

            for row in table.find_all("tr")[1:]:
                cells = row.find_all("td")
                if len(cells) < 5:
                    continue

                commodity_raw = cells[0].get_text(" ", strip=True)
                region = self._normalize_region(cells[1].get_text(" ", strip=True))
                quality = cells[3].get_text(" ", strip=True)
                if self.regions and region not in self.regions:
                    continue

                raw_price_text = cells[2].get_text(" ", strip=True)
                price = self._to_float(
                    raw_price_text.replace("UAH", "").replace(" ", "").replace(",", ".")
                )

                rows.append(
                    {
                        "commodity": commodity_raw,
                        "commodity_group": None,
                        "region": region,
                        "price": price,
                        "currency": "UAH",
                        "unit": "ton",
                        "quality": quality or None,
                        "price_date": self._normalize_date(cells[4].get_text(" ", strip=True)),
                        "source_endpoint": source_endpoint,
                        "source_channel": "web",
                        "note": "ok" if price is not None else "invalid_price",
                        "extracted_at": extracted_at,
                    }
                )
        except Exception as exc:  # noqa: BLE001
            logger.error("APK Inform scraping failed: %s", exc)

        return rows

    def _normalize_rows(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        normalized: List[Dict[str, Any]] = []
        seen: set[tuple[Any, ...]] = set()

        for row in rows:
            commodity_raw = str(row.get("commodity") or "").strip()
            commodity = self.commodity_map.get(commodity_raw, commodity_raw) or None

            record = {
                "commodity": commodity,
                "commodity_group": row.get("commodity_group"),
                "region": row.get("region"),
                "price": self._to_float(row.get("price")),
                "currency": row.get("currency") or "UAH",
                "unit": row.get("unit") or "ton",
                "quality": row.get("quality"),
                "price_date": row.get("price_date"),
                "source_endpoint": row.get("source_endpoint"),
                "source_channel": row.get("source_channel"),
                "note": row.get("note") or "ok",
                "extracted_at": row.get("extracted_at"),
            }

            dedupe_key = (
                record["commodity"],
                record["region"],
                record["price"],
                record["quality"],
                record["price_date"],
            )
            if dedupe_key in seen:
                continue

            seen.add(dedupe_key)
            normalized.append(record)

        normalized.sort(key=lambda item: item.get("price_date") or "", reverse=True)
        return normalized

    @staticmethod
    def _normalize_region(region: Any) -> str | None:
        text = str(region or "").strip()
        if not text:
            return None
        return text if text.startswith("Ukraine-") else f"Ukraine-{text}"

    @staticmethod
    def _normalize_date(value: Any) -> str | None:
        if value is None:
            return None

        text = str(value).strip()
        if not text:
            return None

        for fmt in ("%d.%m.%Y", "%Y-%m-%d", "%d/%m/%Y"):
            try:
                return datetime.strptime(text, fmt).date().isoformat()
            except ValueError:
                continue

        return text

    @staticmethod
    def _to_float(value: Any) -> float | None:
        try:
            if value is None:
                return None
            return float(value)
        except (TypeError, ValueError):
            return None


def api_url_from_base(base_url: str) -> str:
    return f"{base_url}/api/v1/daily-prices"
