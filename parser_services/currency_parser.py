from pathlib import Path
import json
import os
from datetime import datetime, timezone
from typing import List, Dict, Any

import requests

from app.parser_services.base_parser import BaseParser
from app.logger import logger
from app.storage_services import HetznerStorageService, LocalStorageService


CURRENCY_API_MAPPING = {
    "NBU": "https://bank.gov.ua/NBUStatService/v1/statdirectory/exchange?json",
    "PrivatBank": "https://api.privatbank.ua/p24api/pubinfo?json&exchange&coursid=5",
    "ExchangeRateAPI": "https://api.exchangerate-api.com/v4/latest/USD"
}

BASE_OUT_DIR = Path(__file__).resolve().parent.parent.parent
LOCAL_RESULT_DIR = BASE_OUT_DIR / "parsers_results" / "currency"
os.makedirs(LOCAL_RESULT_DIR, exist_ok=True)
TIMESTAMP = ' '.join(datetime.now(timezone.utc).isoformat().split('.')[0].split('T'))
RESULT_PATH = "currency_data_{timestamp}_{provider}.json"


class CurrencyParser(BaseParser):
    def parse(self) -> List[Dict[str, Any]]:
        results: List[Dict[str, Any]] = []
        for provider, api_url in CURRENCY_API_MAPPING.items():
            try:
                logger.info(f"Fetching currency data from API: {api_url}")
                response = requests.get(api_url, timeout=10)
                response.raise_for_status()
                data = response.json()
                logger.info("Currency data fetched successfully for %s", provider)
                results.append({
                    "timestamp": TIMESTAMP,
                    "provider": provider,
                    "data": data,
                })
            except Exception as e:
                logger.error("Error fetching currency data from %s: %s", provider, e)
                continue

        return results

    def save_results(self, results: List[Dict[str, Any]], filepath: str = RESULT_PATH, file_ext: str = "json", storage_type: str = "hetzner") -> None:
        if storage_type == "local":
            for rec in results:
                provider = rec.get('provider', 'unknown').lower()
                local_path = os.path.join(LOCAL_RESULT_DIR, filepath.format(
                    timestamp=TIMESTAMP.replace(':', '-').replace(' ', '_'),
                    provider=provider
                ))
                local_storage = LocalStorageService()
                local_storage.upload_file(file_path=local_path, data=json.dumps(rec.get('data', {}), ensure_ascii=False))

        elif storage_type == "hetzner":
            hetzner = HetznerStorageService()
            for rec in results:
                provider = rec.get('provider', 'unknown').lower()
                timestamped_name = RESULT_PATH.format(
                    timestamp=TIMESTAMP.replace(':', '-').replace(' ', '_'),
                    provider=provider
                )
                temp_filepath = f"/tmp/{provider}_currency_data.{file_ext}"
                with open(temp_filepath, 'w', encoding='utf-8') as f:
                    json.dump(rec.get('data', {}), f, ensure_ascii=False)
                hetzner.upload_file(file_path=temp_filepath, object_name=os.path.basename(timestamped_name))
                os.remove(temp_filepath)
                logger.info("Currency data uploaded to Hetzner: %s", timestamped_name)

        else:
            raise ValueError(f"Unsupported storage type: {storage_type}")


if __name__ == "__main__":
    parser = CurrencyParser()
    results = parser.parse()
    if results:
        # Save both locally and to hetzner by default
        parser.save_results(results=results, storage_type="local")
        try:
            parser.save_results(results=results)
        except Exception as e:
            logger.error("Failed to upload currency data to Hetzner: %s", e)
