from datetime import datetime, timezone
import os
from pathlib import Path

import requests
from bs4 import BeautifulSoup
import pandas as pd
from typing import List, Dict, Any

from app.parser_services.base_parser import BaseParser
from app.logger import logger
from app.storage_services import HetznerStorageService

BASE_DIR = Path(__file__).resolve().parent.parent.parent
BASE_URL = "https://graintrade.com.ua/birzha"
TIMESTAMP = ' '.join(datetime.now(timezone.utc).isoformat().split('.')[0].split('T'))
LOCAL_RESULT_DIR = BASE_DIR / "parsers_results" / "graintradecomua"
os.makedirs(LOCAL_RESULT_DIR, exist_ok=True)
RESULT_PATH = LOCAL_RESULT_DIR / f"graintradecomua_data_{TIMESTAMP.replace(':', '-').replace(' ', '_')}.csv"

class GrainTradeComUaParser(BaseParser):
    def __init__(self, parse_history: bool = True):
        self.url = BASE_URL
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
        }
        self.storage_service = HetznerStorageService()
        self.offers_per_page = 14  # Assuming 14 offers per page
        self.pagination_limit = 10  # Limit number of pages to scrape for demo purposes
        self.page_get_param = "?Ad_page={page_num}"
        self.parse_history = parse_history  # Whether to parse historical offers or just current page

    def parse(self) -> List[Dict[str, Any]]:
        try:
            # Get first page to determine total pages
            response = requests.get(self.url, headers=self.headers)
            if response.status_code != 200:
                logger.error(f"Failed to retrieve page: {response.status_code}")
                return []
            soup = BeautifulSoup(response.text, 'html.parser')
            if self.parse_history:
                # determine total offers and pages
                total_offers = int(soup.find('div', {'class':'summary'}).text.split(' ')[-1].strip(')'))
                logger.info(f"Total offers found: {total_offers}")
                pages = min((total_offers // self.offers_per_page) + 1, self.pagination_limit)
            else:
                pages = 1  # Only current page
            logger.info(f"Total pages to parse: {pages}")

            all_offers = []

            def fetch_page(page_num: int) -> str:
                page_url = self.url + self.page_get_param.format(page_num=page_num)
                resp = requests.get(page_url, headers=self.headers, timeout=10)
                if resp.status_code == 200:
                    logger.info(f"Successfully retrieved page {page_num}")
                    return resp.text
                else:
                    logger.error(f"Failed to retrieve page {page_num}: {resp.status_code}")
                    return ""

            for page in range(1, pages + 1):
                logger.info(f"Fetching page {page} of {pages}")
                page_content = fetch_page(page)
                if page_content:
                    offers = self._parse_offers_from_page(page_content)
                    all_offers.extend(offers)
            
            return all_offers
        except Exception as e:
            logger.error(f"Error during parsing: {e}")
            return []

    def _parse_offers_from_page(self, page_content: str) -> List[Dict[str, Any]]:
        logger.info("Parsing offers from page content.")
        soup = BeautifulSoup(page_content, 'html.parser')
        table = soup.find('table', {'class':'items'})
        if not table:
            logger.warning("Could not find the exchange table on the page.")
            return []

        offers = []
        rows = table.find_all('tr')[1:]  # Skip the header row

        for row in rows:
            cols = row.find_all('td')
            if len(cols) < 8:
                continue  # Skip rows that don't match the expected structure

            offer_data = {
                "date": cols[0].text.strip(),
                "company": cols[1].text.strip(),
                "type": cols[2].text.strip().upper(),  # BUY or SELL
                "crop": cols[3].text.strip(),
                "volume_tons": cols[4].text.strip(),
                "price_raw": cols[5].text.strip(),
                "delivery_term": cols[6].text.strip(),
                "basis": cols[7].text.strip(),  # E.g., CPT, DAP, EXW
                "location": cols[8].text.strip() if len(cols) > 8 else "N/A",
                "details": None,
            }

            # Detail breakdown of the Price
            price_parts = offer_data["price_raw"].split(' ')
            if len(price_parts) >= 2:
                offer_data["price_value"] = price_parts[0]
                offer_data["currency"] = price_parts[1]
            else:
                offer_data["price_value"] = offer_data["price_raw"]
                offer_data["currency"] = "Unknown"

            # Fetch additional details from the detail page
            href = cols[0].find('a')['href'] if cols[0].find('a') else ""
            if href:
                detail_info = self._parse_details_page(href)
                offer_data["details"] = detail_info.get("details", "")

            # Append the parsed offer data
            offers.append(offer_data)
        
        logger.info(f"Parsed {len(offers)} offers from the page.")

        return offers
    
    def _parse_details_page(self, detail_url: str) -> Dict[str, Any]:
        logger.info(f"Fetching detail page: {detail_url}")
        abs_url = detail_url if detail_url.startswith('http') else f"https://graintrade.com.ua{detail_url}"
        try:
            response = requests.get(abs_url, headers=self.headers, timeout=10)
            if response.status_code != 200:
                logger.error(f"Failed to retrieve detail page: {response.status_code}")
                return {}
            soup = BeautifulSoup(response.text, 'html.parser')
            details = ""
            detail_div = soup.find('div', {'class':'addInfoTextView'})
            if detail_div:
                for p in detail_div.find_all('p'):
                    details += p.text.strip() + " "
                if details:
                    logger.info("Detail information fetched successfully.")
            else:
                logger.warning("No detail information found on the page.")

            return {"details": details}
        except Exception as e:
            logger.error(f"Error fetching detail page {abs_url}: {e}")
            return {}

    def save_results(self, results: List[Dict[str, Any]], filepath: str = RESULT_PATH, file_ext: str = "csv", storage_type: str = "hetzner") -> None:
        if not results:
            logger.warning("No results to save.")
            return
        if storage_type == "hetzner":
            logger.info("Saving results to Hetzner storage.")
            
            df = pd.DataFrame(results)

            temp_filepath = f"/tmp/{os.path.basename(filepath)}"
            
            df.to_csv(temp_filepath, index=False, encoding='utf-8')
            self.storage_service.upload_file(file_path=temp_filepath, object_name=os.path.basename(filepath))
            os.remove(temp_filepath)
            logger.info(f"GrainTradeComUa data uploaded to Hetzner: {filepath}")
        elif storage_type == "local":
            logger.info("Saving results locally.")
            
            df = pd.DataFrame(results)
            df.to_csv(filepath, index=False, encoding='utf-8')

            logger.info(f"GrainTradeComUa data saved locally: {filepath}")
        else:
            logger.error(f"Unsupported storage type: {storage_type}")
            raise ValueError(f"Unsupported storage type: {storage_type}")

if __name__ == "__main__":
    parser = GrainTradeComUaParser(parse_history=True)
    results = parser.parse()
    if results:
        parser.save_results(results, storage_type="hetzner")  # Save both locally and to hetzner by default
        parser.save_results(results,  storage_type="local")  # Save both locally and to hetzner by default