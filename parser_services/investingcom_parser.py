from datetime import datetime, timezone, date
import os
import time
from typing import Any, Dict
from pathlib import Path

import investpy
import pandas as pd

from app.parser_services.base_parser import BaseParser
from app.logger import logger
from app.storage_services import HetznerStorageService

BASE_DIR = Path(__file__).resolve().parent.parent.parent
TIMESTAMP = ' '.join(datetime.now(timezone.utc).isoformat().split('.')[0].split('T'))
LOCAL_RESULT_DIR = BASE_DIR / "parsers_results" / "investingcom"
os.makedirs(LOCAL_RESULT_DIR, exist_ok=True)
RESULT_PATH = LOCAL_RESULT_DIR / f"investingcom_data_{TIMESTAMP.replace(':', '-').replace(' ', '_')}.csv"
RESULT_FILENAME = "investingcom_{symbol}_data_{timestamp}.csv"

QUOTES = [
    {"id_": 8917, "name": "US Wheat Futures", "symbol": "ZW", "country": None, "tag": "/commodities/us-wheat", "pair_type": "commodities", "exchange": "ICE"},
    {"id_": 943620, "name": "Golden Wheat", "symbol": "GMC", "country": "palestine", "tag": "/equities/golden-wheat", "pair_type": "stocks", "exchange": "Ramallah"},
    {"id_": 1116094, "name": "ZCE Wheat", "symbol": "CPMc1", "country": None, "tag": "/commodities/zce-wheat-futures", "pair_type": "commodities", "exchange": "ZCE"},
    {"id_": 1116089, "name": "ZCE Strong Gluten Wheat", "symbol": "CWHc1", "country": None, "tag": "/commodities/zce-strong-gluten-wheat-futures", "pair_type": "commodities", "exchange": "ZCE"},
    {"id_": 998243, "name": "Hard Red Winter Wheat Futures", "symbol": "KWc1", "country": None, "tag": "/commodities/hard-red-winter-wheat", "pair_type": "commodities", "exchange": "CME"},
    {"id_": 8912, "name": "London Wheat Futures", "symbol": "LWBc1", "country": None, "tag": "/commodities/london-wheat", "pair_type": "commodities", "exchange": "ICE"},
    {"id_": 1184862, "name": "London Wheat Futures", "symbol": "LWBc2", "country": None, "tag": "/commodities/london-wheat?cid=1184862", "pair_type": "commodities", "exchange": "ICE"},
    {"id_": 1213654, "name": "US Wheat Futures", "symbol": "Wc5", "country": None, "tag": "/commodities/us-wheat?cid=1213654", "pair_type": "commodities", "exchange": "CME"},
    {"id_": 1213653, "name": "US Wheat Futures", "symbol": "Wc4", "country": None, "tag": "/commodities/us-wheat?cid=1213653", "pair_type": "commodities", "exchange": "CME"},
    {"id_": 1213644, "name": "Hard Red Winter Wheat Futures", "symbol": "KWc5", "country": None, "tag": "/commodities/hard-red-winter-wheat?cid=1213644", "pair_type": "commodities", "exchange": "CME"},
    {"id_": 1213643, "name": "Hard Red Winter Wheat Futures", "symbol": "KWc4", "country": None, "tag": "/commodities/hard-red-winter-wheat?cid=1213643", "pair_type": "commodities", "exchange": "CME"},
    {"id_": 1184863, "name": "London Wheat Futures", "symbol": "LWBc3", "country": None, "tag": "/commodities/london-wheat?cid=1184863", "pair_type": "commodities", "exchange": "ICE"},
    {"id_": 1178337, "name": "US Wheat Futures", "symbol": "Wc1", "country": None, "tag": "/commodities/us-wheat?cid=1178337", "pair_type": "commodities", "exchange": "CME"},
    {"id_": 1178339, "name": "US Wheat Futures", "symbol": "Wc3", "country": None, "tag": "/commodities/us-wheat?cid=1178339", "pair_type": "commodities", "exchange": "CME"},
    {"id_": 1178338, "name": "US Wheat Futures", "symbol": "Wc2", "country": None, "tag": "/commodities/us-wheat?cid=1178338", "pair_type": "commodities", "exchange": "CME"},
    {"id_": 1178218, "name": "Hard Red Winter Wheat Futures", "symbol": "KWc3", "country": None, "tag": "/commodities/hard-red-winter-wheat?cid=1178218", "pair_type": "commodities", "exchange": "CME"},
    {"id_": 1178217, "name": "Hard Red Winter Wheat Futures", "symbol": "KWc2", "country": None, "tag": "/commodities/hard-red-winter-wheat?cid=1178217", "pair_type": "commodities", "exchange": "CME"},
    {"id_": 1213655, "name": "US Wheat Futures", "symbol": "Wc6", "country": None, "tag": "/commodities/us-wheat?cid=1213655", "pair_type": "commodities", "exchange": "CME"},

    # Add more quotes as needed
]

class InvestingDataSourceError(RuntimeError):
    """Raised when Investing.com data cannot be retrieved."""

def _format_date(value: datetime) -> str:
    return value.strftime("%d/%m/%Y")

class InvestingComParser(BaseParser):
    def __init__(self, instrument_cfg: Dict[str, Any], start_date: datetime, end_date: datetime):
        self.instrument_cfg = instrument_cfg
        self.start_date = start_date
        self.end_date = end_date
        self.storage_service = HetznerStorageService()

    def parse(self) -> pd.DataFrame:
        try:
            df = fetch_investing_history(
                instrument_cfg=self.instrument_cfg,
                start_date=self.start_date,
                end_date=self.end_date,
            )
            logger.info(f"Fetched {len(df)} records from Investing.com")
            return df
        except Exception as e:
            logger.error(f"Error fetching data from Investing.com: {e}")
            return pd.DataFrame()
        
    def save_results(self, results: pd.DataFrame, filepath: str = RESULT_PATH, file_ext: str = "csv", 
                     storage_type: str = "hetzner") -> None:
        if results.empty:
            logger.warning("No results to save.")
            return 
        if storage_type == "hetzner":
            logger.info("Saving results to Hetzner Storage")

            temp_filepath = f"/tmp/{os.path.basename(filepath)}"

            results.to_csv(temp_filepath, index=False, encoding='utf-8')
            
            self.storage_service.upload_file(file_path=temp_filepath, object_name=os.path.basename(filepath))
            os.remove(temp_filepath)
            logger.info(f"InvestingCom data uploaded to Hetzner: {filepath}")
        elif storage_type == "local":
            logger.info("Saving results locally.")
            results.to_csv(filepath, index=False, encoding='utf-8')
            logger.info(f"InvestingCom data saved locally: {filepath}")
        else:
            logger.error(f"Unsupported storage type: {storage_type}")
            raise ValueError(f"Unsupported storage type: {storage_type}")


def fetch_investing_history(instrument_cfg: Dict[str, Any], start_date: datetime, end_date: datetime) -> pd.DataFrame:
    """Fetch Investing.com historical candles for the configured instrument."""
    instrument_type = instrument_cfg.get("type", "index").lower()
    symbol = instrument_cfg.get("symbol")
    if not symbol:
        raise InvestingDataSourceError("Investing instrument requires a 'symbol'")

    params: Dict[str, Any] = {
        "from_date": _format_date(start_date),
        "to_date": _format_date(end_date),
        "order": "ascending",
    }

    try:
        if instrument_type == "index":
            country = instrument_cfg.get("country", "world")
            frame = investpy.indices.get_index_historical_data(
                index=symbol,
                country=country,
                **params,
            )
        elif instrument_type == "commodity":
            frame = investpy.commodities.get_commodity_historical_data(
                commodity=symbol,
                **params,
            )
        elif instrument_type == "etf":
            country = instrument_cfg.get("country", "united states")
            frame = investpy.etfs.get_etf_historical_data(
                etf=symbol,
                country=country,
                **params,
            )
        elif instrument_type == "currency_cross":
            frame = investpy.currency_crosses.get_currency_cross_historical_data(
                currency_cross=symbol,
                **params,
            )
        else:
            raise InvestingDataSourceError(
                f"Unsupported Investing.com instrument type: {instrument_type}"
            )
    except Exception as exc:  # noqa: BLE001
        logger.error(
            "Failed to download Investing.com history for %s (%s): %s",
            symbol,
            instrument_type,
            exc,
        )
        raise InvestingDataSourceError(str(exc)) from exc

    frame = frame.reset_index().rename(columns={"Date": "date"})
    frame["date"] = pd.to_datetime(frame["date"], utc=True)
    frame["ticker"] = instrument_cfg.get("symbol", symbol)
    return frame


def get_recent_prices_for_ticker(ticker: str) -> pd.DataFrame:
    quote = next((q for q in QUOTES if q["symbol"] == ticker), None)
    if not quote:
        raise ValueError(f"Ticker {ticker} not found in predefined quotes.")
    
    try:
        counter = 0
        search_results = None
        while not search_results or len(search_results) == 0 and counter < 5:
            search_results = investpy.search_quotes(
                text=quote["name"],
                products=[quote["pair_type"]],
                countries=[quote["country"]] if quote["country"] else None,
            )
            counter += 1
            logger.info(f"Attempt {counter}: Searching for {ticker} returned {len(search_results) if search_results else 0} results.")
            if not search_results or len(search_results) == 0:
                logger.warning(f"No search results for {ticker}. Retrying in 5 seconds...")
                time.sleep(5)  # Wait before retrying
        logger.info(f"Search results for {ticker}: {[res.symbol for res in search_results]}")
        for result in search_results:
            if result.symbol == ticker:
                df = result.retrieve_recent_data()  # Fetch recent data
                df = df.reset_index().rename(columns={"Date": "date"})
                df["date"] = pd.to_datetime(df["date"], utc=True)
                df["ticker"] = ticker
                print(df.head())
                return df
    except Exception as e:
        logger.error(f"Failed to get recent data for {ticker} by search: {e}")
    raise InvestingDataSourceError(f"Could not retrieve recent data for ticker {ticker}.")

if __name__ == "__main__":
    # Example usage
    # instrument_cfg = {
    #     "type": "commodity",
    #     "symbol": "Gold",
    # }
    # start_date = datetime(2023, 1, 1)
    # end_date = datetime(2023, 12, 31)

    # parser = InvestingComParser(instrument_cfg, start_date, end_date)
    # data_frame = parser.parse()
    # if not data_frame.empty:
    #     print(data_frame.head())
    #     parser.save_results(data_frame, storage_type="hetzner")
    #     parser.save_results(data_frame, storage_type="local")
    # else:
    #     print("No data fetched from Investing.com.")
    
    # Fetch recent prices for a ticker
    tickers = [quote["symbol"] for quote in QUOTES]
    summary_df = pd.DataFrame()
    for ticker in tickers:
        try:
            recent_df = get_recent_prices_for_ticker(ticker)
            print(f"Recent data for {ticker}:")
            print(recent_df.head())
            summary_df = pd.concat([summary_df, recent_df], ignore_index=True)
        except InvestingDataSourceError as e:
            print(f"Error fetching recent data for {ticker}: {e}")
    
    if not summary_df.empty:
        summary_filepath = LOCAL_RESULT_DIR / f"investingcom_recent_data_{TIMESTAMP.replace(':', '-').replace(' ', '_')}.csv"
        summary_df.to_csv(summary_filepath, index=False, encoding='utf-8')
        print(f"Saved recent data for all tickers to {summary_filepath}")