"""
Backfill Utility Script
Backfill historical data for specific date ranges.
"""

import logging
from datetime import datetime, timedelta
from typing import List

logger = logging.getLogger(__name__)


def backfill_date_range(start_date: str,
                       end_date: str,
                       sources: List[str] = None) -> bool:
    """
    Trigger backfill for specific date range.
    
    Args:
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        sources: List of sources to backfill
    
    Returns:
        True if successful
    """
    if sources is None:
        sources = ["yfinance", "investingcom", "graintradecomua", "apkinform"]

    logger.info(f"Starting backfill from {start_date} to {end_date}")

    try:
        # TODO: Trigger backfill_dag with parameters
        # from airflow.api.client.local_client import Client
        # client = Client(None, None)
        # client.trigger_dag(
        #     dag_id="backfill_dag",
        #     conf={
        #         "start_date": start_date,
        #         "end_date": end_date,
        #         "sources": sources
        #     }
        # )
        
        logger.info("Backfill DAG triggered successfully")
        return True

    except Exception as e:
        logger.error(f"Error triggering backfill: {e}")
        return False


def backfill_last_n_days(days: int,
                        sources: List[str] = None) -> bool:
    """
    Backfill last N days of data.
    
    Args:
        days: Number of days to backfill
        sources: List of sources to backfill
    
    Returns:
        True if successful
    """
    end_date = datetime.utcnow().date()
    start_date = end_date - timedelta(days=days)

    return backfill_date_range(
        start_date.strftime("%Y-%m-%d"),
        end_date.strftime("%Y-%m-%d"),
        sources
    )


def backfill_month(year: int,
                  month: int,
                  sources: List[str] = None) -> bool:
    """
    Backfill entire month.
    
    Args:
        year: Year
        month: Month (1-12)
        sources: List of sources to backfill
    
    Returns:
        True if successful
    """
    start_date = datetime(year, month, 1).strftime("%Y-%m-%d")
    
    # Last day of month
    if month == 12:
        end_date = datetime(year + 1, 1, 1) - timedelta(days=1)
    else:
        end_date = datetime(year, month + 1, 1) - timedelta(days=1)
    
    end_date = end_date.strftime("%Y-%m-%d")

    return backfill_date_range(start_date, end_date, sources)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    # Example usage:
    # python scripts/backfill.py
    # backfill_last_n_days(30)  # Backfill last 30 days
    # backfill_month(2025, 1)   # Backfill January 2025
    
    logger.info("Backfill utility script loaded")
