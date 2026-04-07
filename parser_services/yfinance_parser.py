"""Yahoo Finance parser aligned with project extraction -> bronze staging flow."""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import yfinance as yf
from dotenv import load_dotenv
from pandas import DataFrame

from parser_services.base_parser import BaseParser
from staging.staging_handler import StagingHandler

logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")

COMMODITIES: Dict[str, Dict[str, Any]] = {
    "Wheat Futures": {
        "ticker": "ZW=F",
        "unit": "bushel",
        "kg_per_unit": 27.2155,
        "cents_per_dollar": 100,
        "category": "futures",
        "description": "Wheat Futures (CBOT)",
    },
    "Corn Futures": {
        "ticker": "ZC=F",
        "unit": "bushel",
        "kg_per_unit": 25.4012,
        "cents_per_dollar": 100,
        "category": "futures",
        "description": "Corn Futures (CBOT)",
    },
    "Soybeans Futures": {
        "ticker": "ZS=F",
        "unit": "bushel",
        "kg_per_unit": 27.2155,
        "cents_per_dollar": 100,
        "category": "futures",
        "description": "Soybean Futures (CBOT)",
    },
    "Oats Futures": {
        "ticker": "ZO=F",
        "unit": "bushel",
        "kg_per_unit": 14.515,
        "cents_per_dollar": 100,
        "category": "futures",
        "description": "Oats Futures (CBOT)",
    },
    "Rough Rice Futures": {
        "ticker": "ZR=F",
        "unit": "cwt",
        "kg_per_unit": 45.359237,
        "cents_per_dollar": 100,
        "category": "futures",
        "description": "Rough Rice Futures (CBOT)",
    },
    "Wheat ETF": {
        "ticker": "WEAT",
        "unit": "share",
        "kg_per_unit": None,
        "cents_per_dollar": 1,
        "category": "etf",
        "description": "Wheat ETF",
    },
    "Corn ETF": {
        "ticker": "CORN",
        "unit": "share",
        "kg_per_unit": None,
        "cents_per_dollar": 1,
        "category": "etf",
        "description": "Corn ETF",
    },
    "Soybeans ETF": {
        "ticker": "SOYB",
        "unit": "share",
        "kg_per_unit": None,
        "cents_per_dollar": 1,
        "category": "etf",
        "description": "Soybean ETF",
    },
    "Agricultural Basket": {
        "ticker": "DBA",
        "unit": "share",
        "kg_per_unit": None,
        "cents_per_dollar": 1,
        "category": "etf",
        "description": "Agricultural Basket ETF",
    },
}


class YFinanceParser(BaseParser):
    """Extracts Yahoo Finance market data and stages raw records to bronze."""

    def __init__(self, source_name: str = "yfinance", storage_type: str = "minio") -> None:
        self.source_name = source_name
        self.storage_type = storage_type
        self.bronze_bucket = os.getenv("MINIO_BUCKET_BRONZE", "bronze-layer")
        self.staging_handler = StagingHandler(
            staging_path=self.bronze_bucket,
            storage_type=self.storage_type,
        )

    def parse(self) -> List[Dict[str, Any]]:
        extracted_at = datetime.now(timezone.utc)
        rows: List[Dict[str, Any]] = []

        for name, cfg in COMMODITIES.items():
            price_raw = self._fetch_price(cfg["ticker"])
            if price_raw is None:
                rows.append(
                    {
                        "name": name,
                        "ticker": cfg["ticker"],
                        "category": cfg["category"],
                        "unit": cfg["unit"],
                        "description": cfg["description"],
                        "raw_price": None,
                        "price_in_dollars": None,
                        "usd_per_ton": None,
                        "note": "price_unavailable",
                        "extracted_at": extracted_at.isoformat(),
                    }
                )
                continue

            price_in_dollars = price_raw / cfg.get("cents_per_dollar", 1)
            usd_per_ton = self._convert_to_usd_per_ton(price_in_dollars, cfg.get("kg_per_unit"))

            rows.append(
                {
                    "name": name,
                    "ticker": cfg["ticker"],
                    "category": cfg["category"],
                    "unit": cfg["unit"],
                    "description": cfg["description"],
                    "raw_price": price_raw,
                    "price_in_dollars": price_in_dollars,
                    "usd_per_ton": usd_per_ton,
                    "note": "ok",
                    "extracted_at": extracted_at.isoformat(),
                }
            )

        logger.info("Fetched %s Yahoo Finance records", len(rows))
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
            logger.warning("No Yahoo Finance records to stage")
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
        logger.info("Yahoo Finance raw payload staged to %s", staged_path)

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
        logger.info("Yahoo Finance raw payload staged to %s", staged_path)
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
    def _fetch_price(ticker: str) -> float | None:
        try:
            history = yf.Ticker(ticker).history(period="5d")
            if history.empty:
                logger.warning("No Yahoo data returned for %s", ticker)
                return None

            if ticker == "ZR=F" and len(history) > 1:
                recent_prices = history["Close"].tail(5)
                latest_price = float(recent_prices.iloc[-1])
                avg_previous = float(recent_prices.iloc[:-1].mean())
                if avg_previous and abs(latest_price - avg_previous) / avg_previous > 0.5:
                    return float(recent_prices.iloc[-2])

            return float(history["Close"].iloc[-1])
        except Exception as exc:  # noqa: BLE001
            logger.error("Yahoo fetch failed for %s: %s", ticker, exc)
            return None

    @staticmethod
    def _convert_to_usd_per_ton(price: float | None, kg_per_unit: float | None) -> float | None:
        if price is None or kg_per_unit is None:
            return None
        return price * (1000.0 / kg_per_unit)
