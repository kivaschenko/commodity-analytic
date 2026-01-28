"""
APK-Inform Parser - Ukrainian agricultural market data source
Website: https://www.apk-inform.com/en/prices

This parser fetches grain commodity prices from APK-Inform,
a leading agricultural analytics platform in Ukraine.
"""
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict, Any
import os

import pandas as pd
import requests
from bs4 import BeautifulSoup

from app.parser_services.base_parser import BaseParser
from app.logger import logger
from app.storage_services import HetznerStorageService

BASE_DIR = Path(__file__).resolve().parent.parent.parent
TIMESTAMP = datetime.now(timezone.utc).isoformat().split('.')[0].replace(':', '-').replace('T', '_')
LOCAL_RESULT_DIR = BASE_DIR / "parsers_results" / "apk_inform"
os.makedirs(LOCAL_RESULT_DIR, exist_ok=True)
RESULT_PATH = LOCAL_RESULT_DIR / f"apk_inform_data_{TIMESTAMP}.csv"


class APKInformParser(BaseParser):
    """
    Parser for APK-Inform Ukrainian grain market prices.
    
    Simple implementation following KISS principle:
    - Scrapes public price tables
    - Normalizes to standard schema
    - Saves to CSV and optionally uploads to Hetzner S3
    """
    
    def __init__(self, regions: List[str] = None):
        """
        Initialize parser.
        
        :param regions: List of Ukrainian regions to parse (e.g., ['Odesa', 'Mykolaiv'])
                       If None, parses all available regions.
        """
        self.base_url = "https://www.apk-inform.com/en/prices"
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
        self.regions = regions or []
        self.storage_service = HetznerStorageService()
        
        # Commodity name mapping (APK-Inform -> Standard)
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
    
    def parse(self) -> pd.DataFrame:
        """
        Parse grain prices from APK-Inform.
        
        Returns:
            DataFrame with columns: commodity, region, price, currency, unit, quality, date, source
        """
        logger.info("Starting APK-Inform parser")
        all_prices = []
        
        try:
            # Method 1: Try API endpoint (if available)
            api_data = self._fetch_from_api()
            if api_data:
                all_prices.extend(api_data)
            else:
                # Method 2: Fallback to web scraping
                scraped_data = self._scrape_prices()
                all_prices.extend(scraped_data)
            
            if not all_prices:
                logger.warning("No data parsed from APK-Inform")
                return pd.DataFrame()
            
            # Convert to DataFrame
            df = pd.DataFrame(all_prices)
            
            # Normalize and clean
            df = self._normalize_data(df)
            
            # Save to CSV
            df.to_csv(RESULT_PATH, index=False, encoding='utf-8')
            logger.info(f"Saved APK-Inform data to {RESULT_PATH}")
            
            # Upload to Hetzner S3 (if configured)
            self._upload_to_storage(RESULT_PATH)
            
            logger.info(f"Parsed {len(df)} price records from APK-Inform")
            return df
            
        except Exception as exc:
            logger.error(f"APK-Inform parsing failed: {exc}")
            return pd.DataFrame()
    
    def _fetch_from_api(self) -> List[Dict[str, Any]]:
        """
        Attempt to fetch data from APK-Inform API (if available).
        
        NOTE: This is a placeholder. Real implementation would require
        API key and documentation from APK-Inform.
        """
        # Example structure - adjust based on actual APK-Inform API
        api_url = f"{self.base_url}/api/v1/daily-prices"  # Hypothetical
        
        try:
            response = requests.get(api_url, headers=self.headers, timeout=10)
            if response.status_code == 200:
                data = response.json()
                # Parse JSON response
                return self._parse_api_response(data)
        except Exception as exc:
            logger.debug(f"API fetch failed, will try scraping: {exc}")
        
        return []
    
    def _parse_api_response(self, data: dict) -> List[Dict[str, Any]]:
        """Parse API JSON response into standard format."""
        prices = []
        # Example implementation - adjust based on actual API structure
        for item in data.get('prices', []):
            prices.append({
                'commodity': item.get('commodity_name'),
                'region': item.get('region'),
                'price': item.get('price'),
                'currency': item.get('currency', 'UAH'),
                'unit': item.get('unit', 'ton'),
                'quality': item.get('quality'),
                'date': item.get('date'),
                'source': 'APK-Inform API',
            })
        return prices
    
    def _scrape_prices(self) -> List[Dict[str, Any]]:
        """
        Scrape prices from APK-Inform website.
        
        This is a simplified example. Real implementation would need
        to handle pagination, authentication, and dynamic content.
        """
        prices = []
        
        try:
            response = requests.get(f"{self.base_url}/grains", headers=self.headers, timeout=15)
            if response.status_code != 200:
                logger.error(f"Failed to fetch APK-Inform page: {response.status_code}")
                return []
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Find price table (adjust selector based on actual HTML structure)
            price_table = soup.find('table', {'class': 'prices-table'})  # Example
            if not price_table:
                logger.warning("Could not find price table on APK-Inform page")
                return []
            
            # Parse table rows
            rows = price_table.find_all('tr')[1:]  # Skip header
            for row in rows:
                cells = row.find_all('td')
                if len(cells) < 5:
                    continue
                
                commodity_raw = cells[0].get_text(strip=True)
                region = cells[1].get_text(strip=True)
                price_str = cells[2].get_text(strip=True).replace(',', '').replace(' UAH', '')
                quality = cells[3].get_text(strip=True)
                date_str = cells[4].get_text(strip=True)
                
                # Filter by region if specified
                if self.regions and region not in self.regions:
                    continue
                
                try:
                    price = float(price_str)
                except ValueError:
                    continue
                
                # Parse date
                try:
                    date = datetime.strptime(date_str, '%d.%m.%Y').date()
                except ValueError:
                    date = datetime.now(timezone.utc).date()
                
                prices.append({
                    'commodity': commodity_raw,
                    'region': f"Ukraine-{region}",
                    'price': price,
                    'currency': 'UAH',
                    'unit': 'ton',
                    'quality': quality,
                    'date': date,
                    'source': 'APK-Inform Web',
                })
        
        except Exception as exc:
            logger.error(f"Web scraping failed: {exc}")
        
        return prices
    
    def _normalize_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Normalize parsed data to standard schema.
        """
        # Map commodity names
        df['commodity'] = df['commodity'].map(self.commodity_map).fillna(df['commodity'])
        
        # Ensure date is datetime
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'])
        
        # Remove duplicates
        df = df.drop_duplicates(subset=['commodity', 'region', 'date', 'quality'])
        
        # Sort by date
        df = df.sort_values('date', ascending=False)
        
        return df
    
    def _upload_to_storage(self, file_path: Path):
        """Upload result file to Hetzner S3 storage."""
        try:
            self.storage_service.upload(
                str(file_path),
                f"apk_inform/{file_path.name}"
            )
            logger.info(f"Uploaded {file_path.name} to Hetzner storage")
        except Exception as exc:
            logger.warning(f"Failed to upload to Hetzner: {exc}")


def main():
    """Run parser standalone for testing."""
    parser = APKInformParser(regions=['Odesa', 'Mykolaiv', 'Kherson'])
    df = parser.parse()
    
    if not df.empty:
        print(f"\nParsed {len(df)} records:")
        print(df.head(10))
        print(f"\nCommodities: {df['commodity'].unique().tolist()}")
        print(f"Regions: {df['region'].unique().tolist()}")
        print(f"Date range: {df['date'].min()} to {df['date'].max()}")
    else:
        print("No data parsed")


if __name__ == "__main__":
    main()
