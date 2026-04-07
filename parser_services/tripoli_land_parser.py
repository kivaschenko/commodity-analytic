"""Tripoli Land parser aligned with extraction -> bronze staging flow."""

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

BASE_URL = "https://tripoli.land"
BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")

COMPANIES: Dict[str, str] = {
    "nibulon": "Нібулон",
    "kernel": "Кернел",
    "lnz-group": "ЛНЗ Груп",
    "tas-agro": "ТАС АГРО",
    "astarta-kiev": "Астарта-Київ",
    "mhp": "МХП",
    "agroprosperis": "Агропросперіс (NCH)",
    "prodinvest-servis": "Продінвест-Сервіс",
    "prometey": "Прометей",
    "expograin": "Експогрейн",
    "mko-trans-servis": "МКО-Транс-Сервіс",
    "ukrzernoinvest-2013": "Укрзернінвест-2013",
    "ooo-spoteks-treyd": "СПОТЕКС-ТРЕЙД",
    "agromino": "Агроміно",
    "ags-grain": "АГС-ГРЕЙН",
    "greynheven": "Грейнхевен",
    "bruklin-kiev": "Бруклін-Київ",
    "ramburs": "Рамбурс",
    "kortiya-ukraina": "Кортія-Україна",
    "glenport": "Гленпорт",
    "ooo-vvt-grupp": "ВВТ-Груп",
    "almeyda-grup-almeida-group": "Алмейда Груп",
    "ooo-agrotreyd-2022": "Агротрейд 2022",
    "dileks-treyd-dilex-trade": "Ділекс Трейд",
}

GRAIN_NAME_MAPPING: Dict[str, str] = {
    "Кукурудза": "Corn",
    "Кукурудза фуражна": "Corn",
    "Кукурудза кремниста": "Corn",
    "Пшениця 1 клас": "Wheat 1st grade",
    "Пшениця 2 клас": "Wheat 2nd grade",
    "Пшениця 3 клас": "Wheat 3rd grade",
    "Пшениця 4 клас": "Wheat 4th grade",
    "Пшениця 5 клас": "Wheat 5th grade",
    "Пшениця фуражна": "Wheat 4th grade",
    "Пшениця неконд.": "Wheat 6th grade",
    "Пшениця тверда": "Durum wheat",
    "Ячмінь": "Barley",
    "Овес": "Oats",
    "Овес голозернистий": "Oats",
    "Рис": "Rice",
    "Жито": "Rye",
    "Тритікале": "Triticale",
    "Просо жовте": "Millet",
    "Просо біле": "Millet",
    "Просо червоне": "Millet",
    "Гречка": "Buckwheat",
    "Полба": "Einkorn wheat",
    "Соняшник": "Sunflower",
    "Соняшник високоолеїновий": "Sunflower high-oleic",
    "Соняшник кондитерський": "Confectionery sunflower seeds",
    "Ріпак": "Rapeseed",
    "Ріпак без гмо": "Rapeseed",
    "Горох зелений": "Green peas",
    "Горох жовтий": "Peas",
    "Люпин": "Lupine",
    "Боби": "White kidney beans",
    "Квасоля": "White kidney beans",
    "Нут": "Chick-peas",
    "Сочевиця": "Lentil",
    "Сочевиця зелена": "Lentil",
    "Соя": "Soybeans",
    "Соя без гмо": "Soybeans GMO-free",
    "Шрот соняшниковий": "Sunflower seed meal",
    "Жмих соняшниковий": "Sunflower oil cake",
    "Шрот соєвий": "Soybean meal",
    "Жмих сої": "Soybean cake",
    "Жмих ріпаку": "Rapeseed cake",
    "Шрот ріпаку": "Rapeseed coarse meal",
    "Жмих кукурудзи": "Corn cake",
    "Висівки пшениці": "Wheat mill offals",
    "Висівки кукурудзи": "Corn mill offals",
    "Олія соняшникова": "Sunflower oil",
    "Олія соєва": "Soybean oil",
    "Олія ріпакова": "Rapeseed oil",
    "Олія кукурудзяна": "Corn oil",
    "Борошно": "Wheat flour class 1",
    "Цукор": "Sugar",
    "Переробка сої": "Soy processing",
    "Сорго біле": "Sorghum white",
    "Сорго червоне": "Sorghum red",
    "Віка": "Vetch",
    "Гірчиця жовта": "Mustard seeds",
    "Гірчиця біла": "Mustard seeds",
    "Гірчиця чорна": "Mustard seeds",
    "Коріандр": "Coriander",
    "Льон": "Flax",
    "Льон золотий": "Flax",
    "Люцерна": "Lucerne",
    "Очеретянка": "Other",
}


class TripoliLandParser(BaseParser):
    """Extracts grain offer prices from Tripoli Land and stages raw records to bronze."""

    def __init__(self, source_name: str = "tripoli_land", storage_type: str = "minio") -> None:
        self.source_name = source_name
        self.storage_type = storage_type
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
        rows: List[Dict[str, Any]] = []

        for company_slug, company_name in COMPANIES.items():
            company_rows = self._parse_company(company_slug, company_name, extracted_at)
            if not company_rows:
                rows.append(
                    {
                        "company_slug": company_slug,
                        "company_name": company_name,
                        "storage_type": None,
                        "storage_location": None,
                        "address_region": None,
                        "culture_ua": None,
                        "category_name": None,
                        "price": None,
                        "price_uah_per_ton": None,
                        "source_url": f"{BASE_URL}/ua/companies/{company_slug}",
                        "note": "company_unavailable_or_empty",
                        "extracted_at": extracted_at,
                    }
                )
                continue

            rows.extend(company_rows)

        logger.info("Fetched %s Tripoli Land records", len(rows))
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
            logger.warning("No Tripoli Land records to stage")
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
        logger.info("Tripoli Land raw payload staged to %s", staged_path)

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
        logger.info("Tripoli Land raw payload staged to %s", staged_path)
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

    def _parse_company(
        self,
        company_slug: str,
        company_name: str,
        extracted_at: str,
    ) -> List[Dict[str, Any]]:
        url = f"{BASE_URL}/ua/companies/{company_slug}"
        try:
            response = requests.get(url, headers=self.headers, timeout=20)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")
            tables = soup.find_all("table")

            records: List[Dict[str, Any]] = []
            for table in tables:
                header_row = table.find("tr")
                storage_type = self._extract_storage_type(header_row)
                if storage_type not in {"Порт", "Елеватор"}:
                    continue
                if header_row is None:
                    continue

                columns = [
                    th.text.strip()
                    for th in header_row.find_all(["th", "td"])[1:]
                ]

                for row in table.find_all("tr")[1:]:
                    cells = row.find_all("td")
                    if len(cells) < 2:
                        continue

                    location_cell = cells[0]
                    storage_location = location_cell.b.text.strip() if location_cell.b else ""
                    address_region = location_cell.p.text.strip() if location_cell.p else ""

                    for idx in range(1, len(cells)):
                        if idx >= len(columns):
                            continue

                        culture_ua = columns[idx].strip()
                        raw_price = cells[idx].text.strip().replace(" ", "")
                        if not raw_price or raw_price == "-":
                            continue

                        numeric_price = self._to_float(raw_price.replace(",", "."))

                        records.append(
                            {
                                "company_slug": company_slug,
                                "company_name": company_name,
                                "storage_type": storage_type,
                                "storage_location": storage_location,
                                "address_region": address_region,
                                "culture_ua": culture_ua,
                                "category_name": GRAIN_NAME_MAPPING.get(culture_ua, culture_ua),
                                "price": raw_price,
                                "price_uah_per_ton": numeric_price,
                                "source_url": url,
                                "note": "ok",
                                "extracted_at": extracted_at,
                            }
                        )

            return records
        except Exception as exc:  # noqa: BLE001
            logger.error("Tripoli parsing failed for %s: %s", company_slug, exc)
            return []

    @staticmethod
    def _extract_storage_type(header_row: Any) -> str:
        if not header_row:
            return ""
        title_cell = header_row.find("th")
        if not title_cell:
            return ""
        return title_cell.text.strip()

    @staticmethod
    def _to_float(value: Any) -> float | None:
        try:
            if value is None:
                return None
            return float(value)
        except (TypeError, ValueError):
            return None
