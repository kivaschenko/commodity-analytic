"""
Data Enricher - Adds business context and derived columns.
Enriches data with dimensions, calculated fields, and historical context.
"""

import logging
from typing import List, Dict, Any
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class DataEnricher:
    """
    Enriches commodity data with additional context:
    - Add date/time dimensions (year, month, quarter, day of week)
    - Market context flags (holidays, trading halts)
    - Calculate derived metrics (price changes, volatility)
    - Add commodity attributes
    - Historical comparisons (YoY, MoM)
    """

    def __init__(self):
        self.enrichment_log = []
        self.holidays = self._load_holidays()

    def _load_holidays(self) -> set:
        """Load major trading holidays."""
        return {
            "2025-01-01",  # New Year
            "2025-12-25",  # Christmas
            "2025-07-04",  # US Independence Day
        }

    def add_date_dimensions(self, data: List[Dict],
                           date_column: str = "date") -> List[Dict]:
        """
        Add time dimensions to records.
        
        Args:
            data: Records with date column
            date_column: Column containing date
        
        Returns:
            Data with added dimensions (year, month, quarter, etc.)
        """
        enriched = []

        for record in data:
            enriched_record = record.copy()
            
            if date_column in record:
                try:
                    if isinstance(record[date_column], str):
                        date_obj = datetime.strptime(record[date_column], "%Y-%m-%d")
                    else:
                        date_obj = record[date_column]

                    enriched_record["year"] = date_obj.year
                    enriched_record["month"] = date_obj.month
                    enriched_record["quarter"] = (date_obj.month - 1) // 3 + 1
                    enriched_record["day_of_week"] = date_obj.strftime("%A")
                    enriched_record["week_of_year"] = date_obj.isocalendar()[1]
                    enriched_record["is_weekend"] = date_obj.weekday() >= 5

                except Exception as e:
                    logger.warning(f"Failed to parse date: {record[date_column]}")

            enriched.append(enriched_record)

        self.enrichment_log.append({
            "operation": "add_date_dimensions",
            "rows_enriched": len(enriched)
        })

        return enriched

    def add_trading_flags(self, data: List[Dict],
                         date_column: str = "date") -> List[Dict]:
        """
        Add flags for trading conditions.
        
        Args:
            data: Records with date
            date_column: Column containing date
        
        Returns:
            Data with trading flags
        """
        enriched = []

        for record in data:
            enriched_record = record.copy()
            
            if date_column in record:
                date_str = record[date_column]
                enriched_record["is_holiday"] = date_str in self.holidays
                enriched_record["trading_status"] = "halted" if date_str in self.holidays else "active"

            enriched.append(enriched_record)

        self.enrichment_log.append({
            "operation": "add_trading_flags",
            "rows_enriched": len(enriched)
        })

        return enriched

    def add_price_changes(self, data: List[Dict],
                         price_column: str = "price") -> List[Dict]:
        """
        Calculate day-over-day and percent changes.
        
        Args:
            data: Records sorted by date, must contain price
            price_column: Column with price values
        
        Returns:
            Data with calculated price changes
        """
        enriched = []
        previous_price = None

        for record in data:
            enriched_record = record.copy()
            
            if price_column in record:
                current_price = record[price_column]
                
                if previous_price is not None:
                    price_change = current_price - previous_price
                    price_change_pct = (price_change / previous_price * 100) if previous_price != 0 else 0
                    
                    enriched_record["price_change"] = round(price_change, 2)
                    enriched_record["price_change_pct"] = round(price_change_pct, 2)
                else:
                    enriched_record["price_change"] = None
                    enriched_record["price_change_pct"] = None

                previous_price = current_price

            enriched.append(enriched_record)

        self.enrichment_log.append({
            "operation": "add_price_changes",
            "rows_enriched": len(enriched)
        })

        return enriched

    def add_commodity_attributes(self, data: List[Dict],
                                commodity_column: str = "commodity",
                                attributes_map: Dict[str, Dict] = None) -> List[Dict]:
        """
        Add commodity metadata attributes.
        
        Args:
            data: Records with commodity column
            commodity_column: Column with commodity name
            attributes_map: Dict mapping commodity names to attributes
        
        Returns:
            Data with commodity attributes
        """
        if attributes_map is None:
            attributes_map = {
                "Wheat": {"type": "grain", "category": "cereals", "unit": "ton"},
                "Corn": {"type": "grain", "category": "cereals", "unit": "ton"},
                "Soybeans": {"type": "oil_crop", "category": "legumes", "unit": "ton"},
                "Barley": {"type": "grain", "category": "cereals", "unit": "ton"},
                "Rapeseed": {"type": "oil_crop", "category": "oil_seeds", "unit": "ton"},
            }

        enriched = []

        for record in data:
            enriched_record = record.copy()
            
            if commodity_column in record:
                commodity = record[commodity_column]
                if commodity in attributes_map:
                    attrs = attributes_map[commodity]
                    enriched_record.update({
                        f"commodity_{k}": v for k, v in attrs.items()
                    })

            enriched.append(enriched_record)

        self.enrichment_log.append({
            "operation": "add_commodity_attributes",
            "rows_enriched": len(enriched)
        })

        return enriched

    def get_enrichment_report(self) -> Dict[str, Any]:
        """Get summary of all enrichment operations."""
        return {
            "operations_count": len(self.enrichment_log),
            "operations": self.enrichment_log
        }
