"""GrainTrade.com.ua parser aligned with extraction -> bronze staging flow."""

from __future__ import annotations

import logging
import os
import re
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

BASE_URL = "https://graintrade.com.ua/birzha"
SITE_ORIGIN = "https://graintrade.com.ua"
BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")


class GrainTradeComUaParser(BaseParser):
    """Extracts grain offers from GrainTrade.com.ua and stages raw records to bronze."""

    def __init__(
        self,
        source_name: str = "graintradecomua",
        storage_type: str = "minio",
        parse_history: bool = True,
        pagination_limit: int = 10,
    ) -> None:
        self.source_name = source_name
        self.storage_type = storage_type
        self.parse_history = parse_history
        self.pagination_limit = pagination_limit
        self.offers_per_page = 14
        self.bronze_bucket = os.getenv("MINIO_BUCKET_BRONZE", "bronze-layer")
        self.staging_handler = StagingHandler(
            staging_path=self.bronze_bucket,
            storage_type=self.storage_type,
        )
        self.headers = {
            "User-Agent": (
                "Mozilla/5.0 (X11; Linux x86_64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
            )
        }

    def parse(self) -> List[Dict[str, Any]]:
        extracted_at = datetime.now(timezone.utc).isoformat()
        first_page = self._fetch_page(1)
        if not first_page:
            return []

        pages = self._resolve_pages(first_page) if self.parse_history else 1
        rows: List[Dict[str, Any]] = []

        for page in range(1, pages + 1):
            page_content = first_page if page == 1 else self._fetch_page(page)
            if not page_content:
                rows.append(
                    {
                        "source_url": self._page_url(page),
                        "page": page,
                        "offer_date": None,
                        "company": None,
                        "offer_type": None,
                        "crop": None,
                        "volume_tons": None,
                        "price_raw": None,
                        "price_value": None,
                        "currency": None,
                        "delivery_term": None,
                        "basis": None,
                        "location": None,
                        "details": None,
                        "detail_url": None,
                        "note": "page_unavailable",
                        "extracted_at": extracted_at,
                    }
                )
                continue

            page_rows = self._parse_offers_from_page(page_content, page, extracted_at)
            if not page_rows:
                rows.append(
                    {
                        "source_url": self._page_url(page),
                        "page": page,
                        "offer_date": None,
                        "company": None,
                        "offer_type": None,
                        "crop": None,
                        "volume_tons": None,
                        "price_raw": None,
                        "price_value": None,
                        "currency": None,
                        "delivery_term": None,
                        "basis": None,
                        "location": None,
                        "details": None,
                        "detail_url": None,
                        "note": "page_without_offers",
                        "extracted_at": extracted_at,
                    }
                )
                continue

            rows.extend(page_rows)

        logger.info("Fetched %s GrainTrade.com.ua records", len(rows))
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
            logger.warning("No GrainTrade.com.ua records to stage")
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
        logger.info("GrainTrade.com.ua raw payload staged to %s", staged_path)

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
        logger.info("GrainTrade.com.ua raw payload staged to %s", staged_path)
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

    def _fetch_page(self, page_num: int) -> str:
        try:
            response = requests.get(self._page_url(page_num), headers=self.headers, timeout=20)
            response.raise_for_status()
            return response.text
        except Exception as exc:  # noqa: BLE001
            logger.error("GrainTrade page fetch failed for page %s: %s", page_num, exc)
            return ""

    def _resolve_pages(self, first_page_content: str) -> int:
        soup = BeautifulSoup(first_page_content, "html.parser")
        summary = soup.find("div", {"class": "summary"})
        if not summary:
            return 1

        digits = re.findall(r"\d+", summary.get_text(" ", strip=True))
        if not digits:
            return 1

        total_offers = int(digits[-1])
        estimated_pages = (total_offers // self.offers_per_page) + 1
        pages = min(max(estimated_pages, 1), self.pagination_limit)
        logger.info("GrainTrade summary detected %s offers, parsing %s pages", total_offers, pages)
        return pages

    def _parse_offers_from_page(
        self,
        page_content: str,
        page_num: int,
        extracted_at: str,
    ) -> List[Dict[str, Any]]:
        soup = BeautifulSoup(page_content, "html.parser")
        table = soup.find("table", {"class": "items"})
        if not table:
            return []

        offers: List[Dict[str, Any]] = []
        rows = table.find_all("tr")[1:]

        for row in rows:
            cols = row.find_all("td")
            if len(cols) < 8:
                continue

            detail_url = None
            date_anchor = cols[0].find("a")
            if date_anchor and date_anchor.get("href"):
                href = date_anchor.get("href")
                if isinstance(href, str):
                    detail_url = self._absolute_url(href)

            price_raw = cols[5].get_text(" ", strip=True)
            price_value, currency = self._extract_price(price_raw)
            details = self._parse_details_page(detail_url) if detail_url else ""

            offers.append(
                {
                    "source_url": self._page_url(page_num),
                    "page": page_num,
                    "offer_date": cols[0].get_text(" ", strip=True),
                    "company": cols[1].get_text(" ", strip=True),
                    "offer_type": cols[2].get_text(" ", strip=True).upper(),
                    "crop": cols[3].get_text(" ", strip=True),
                    "volume_tons": cols[4].get_text(" ", strip=True),
                    "price_raw": price_raw,
                    "price_value": price_value,
                    "currency": currency,
                    "delivery_term": cols[6].get_text(" ", strip=True),
                    "basis": cols[7].get_text(" ", strip=True),
                    "location": cols[8].get_text(" ", strip=True) if len(cols) > 8 else None,
                    "details": details,
                    "detail_url": detail_url,
                    "note": "ok",
                    "extracted_at": extracted_at,
                }
            )

        return offers

    def _parse_details_page(self, detail_url: str) -> str:
        try:
            response = requests.get(detail_url, headers=self.headers, timeout=20)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")
            detail_div = soup.find("div", {"class": "addInfoTextView"})
            if not detail_div:
                return ""

            paragraphs = [p.get_text(" ", strip=True) for p in detail_div.find_all("p")]
            return " ".join([text for text in paragraphs if text])
        except Exception as exc:  # noqa: BLE001
            logger.warning("GrainTrade detail fetch failed for %s: %s", detail_url, exc)
            return ""

    @staticmethod
    def _extract_price(raw_price: str) -> tuple[float | None, str | None]:
        if not raw_price:
            return None, None

        cleaned = raw_price.replace("\xa0", " ").strip()
        number_match = re.search(r"\d+[\d\s,\.]*", cleaned)
        if not number_match:
            return None, None

        value_text = number_match.group(0).replace(" ", "").replace(",", ".")
        currency_text = cleaned[number_match.end():].strip() or None
        return GrainTradeComUaParser._to_float(value_text), currency_text

    @staticmethod
    def _to_float(value: Any) -> float | None:
        try:
            if value is None:
                return None
            return float(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _absolute_url(path_or_url: str) -> str:
        if path_or_url.startswith("http"):
            return path_or_url
        return f"{SITE_ORIGIN}{path_or_url}"

    @staticmethod
    def _page_url(page_num: int) -> str:
        return f"{BASE_URL}?Ad_page={page_num}"