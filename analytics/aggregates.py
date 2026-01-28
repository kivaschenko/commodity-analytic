"""
Aggregate Builder - Creates summary tables and materialized views for performance.
"""

import logging
from typing import List, Dict, Any
from datetime import datetime

logger = logging.getLogger(__name__)


class AggregateBuilder:
    """
    Builds materialized aggregate tables:
    - Daily price summaries (OHLCV)
    - Weekly and monthly summaries
    - Commodity-specific aggregates
    - Market-specific aggregates
    """

    def __init__(self):
        self.aggregation_log = []

    def build_daily_summary(self, data: List[Dict],
                           date_column: str = "date",
                           commodity_column: str = "commodity",
                           market_column: str = "market") -> List[Dict]:
        """
        Build daily OHLCV summaries.
        
        Args:
            data: Fact-level commodity price data
            date_column: Date column name
            commodity_column: Commodity column name
            market_column: Market column name
        
        Returns:
            Daily summary records
        """
        summaries = {}

        # Group by date, commodity, market
        for record in data:
            key = (
                record.get(date_column),
                record.get(commodity_column),
                record.get(market_column)
            )

            if key not in summaries:
                summaries[key] = {
                    "date": key[0],
                    "commodity": key[1],
                    "market": key[2],
                    "prices": [],
                    "volumes": [],
                }

            if "close_price" in record:
                summaries[key]["prices"].append(record["close_price"])
            if "volume" in record:
                summaries[key]["volumes"].append(record["volume"])

        # Calculate summaries
        summary_records = []
        for key, agg_data in summaries.items():
            prices = agg_data["prices"]
            volumes = agg_data["volumes"]

            summary = {
                "date": agg_data["date"],
                "commodity": agg_data["commodity"],
                "market": agg_data["market"],
                "open_price": prices[0] if prices else None,
                "close_price": prices[-1] if prices else None,
                "high_price": max(prices) if prices else None,
                "low_price": min(prices) if prices else None,
                "volume": sum(volumes) if volumes else 0,
                "record_count": len(prices),
            }

            if prices and len(prices) > 1:
                summary["price_change"] = prices[-1] - prices[0]
                summary["price_change_pct"] = (
                    (prices[-1] - prices[0]) / prices[0] * 100
                )

            summary_records.append(summary)

        self.aggregation_log.append({
            "operation": "build_daily_summary",
            "summaries_created": len(summary_records),
            "records_processed": len(data)
        })

        return summary_records

    def build_weekly_summary(self, daily_summaries: List[Dict],
                            date_column: str = "date",
                            commodity_column: str = "commodity") -> List[Dict]:
        """
        Build weekly summaries from daily data.
        
        Args:
            daily_summaries: Daily summary records
            date_column: Date column
            commodity_column: Commodity column
        
        Returns:
            Weekly summary records
        """
        summaries = {}

        for record in daily_summaries:
            try:
                if isinstance(record[date_column], str):
                    date_obj = datetime.strptime(record[date_column], "%Y-%m-%d")
                else:
                    date_obj = record[date_column]

                year = date_obj.year
                week = date_obj.isocalendar()[1]
                commodity = record.get(commodity_column)

                key = (year, week, commodity)

                if key not in summaries:
                    summaries[key] = {
                        "year": year,
                        "week": week,
                        "commodity": commodity,
                        "prices": [],
                        "volumes": [],
                    }

                if "close_price" in record:
                    summaries[key]["prices"].append(record["close_price"])
                if "volume" in record:
                    summaries[key]["volumes"].append(record["volume"])

            except Exception as e:
                logger.warning(f"Failed to aggregate record: {e}")

        # Calculate summaries
        summary_records = []
        for key, agg_data in summaries.items():
            prices = agg_data["prices"]
            volumes = agg_data["volumes"]

            summary = {
                "year": agg_data["year"],
                "week": agg_data["week"],
                "commodity": agg_data["commodity"],
                "avg_price": sum(prices) / len(prices) if prices else None,
                "min_price": min(prices) if prices else None,
                "max_price": max(prices) if prices else None,
                "total_volume": sum(volumes) if volumes else 0,
                "record_count": len(prices),
            }

            summary_records.append(summary)

        self.aggregation_log.append({
            "operation": "build_weekly_summary",
            "summaries_created": len(summary_records),
            "records_processed": len(daily_summaries)
        })

        return summary_records

    def build_monthly_summary(self, daily_summaries: List[Dict],
                             date_column: str = "date",
                             commodity_column: str = "commodity") -> List[Dict]:
        """
        Build monthly summaries from daily data.
        
        Args:
            daily_summaries: Daily summary records
            date_column: Date column
            commodity_column: Commodity column
        
        Returns:
            Monthly summary records
        """
        summaries = {}

        for record in daily_summaries:
            try:
                if isinstance(record[date_column], str):
                    date_obj = datetime.strptime(record[date_column], "%Y-%m-%d")
                else:
                    date_obj = record[date_column]

                year = date_obj.year
                month = date_obj.month
                commodity = record.get(commodity_column)

                key = (year, month, commodity)

                if key not in summaries:
                    summaries[key] = {
                        "year": year,
                        "month": month,
                        "commodity": commodity,
                        "prices": [],
                        "volumes": [],
                    }

                if "close_price" in record:
                    summaries[key]["prices"].append(record["close_price"])
                if "volume" in record:
                    summaries[key]["volumes"].append(record["volume"])

            except Exception as e:
                logger.warning(f"Failed to aggregate record: {e}")

        # Calculate summaries
        summary_records = []
        for key, agg_data in summaries.items():
            prices = agg_data["prices"]
            volumes = agg_data["volumes"]

            price_volatility = None
            if prices and len(prices) > 1:
                avg_price = sum(prices) / len(prices)
                variance = sum((p - avg_price) ** 2 for p in prices) / len(prices)
                price_volatility = variance ** 0.5

            summary = {
                "year": agg_data["year"],
                "month": agg_data["month"],
                "commodity": agg_data["commodity"],
                "avg_price": sum(prices) / len(prices) if prices else None,
                "min_price": min(prices) if prices else None,
                "max_price": max(prices) if prices else None,
                "total_volume": sum(volumes) if volumes else 0,
                "price_volatility": price_volatility,
                "record_count": len(prices),
            }

            summary_records.append(summary)

        self.aggregation_log.append({
            "operation": "build_monthly_summary",
            "summaries_created": len(summary_records),
            "records_processed": len(daily_summaries)
        })

        return summary_records

    def get_aggregation_report(self) -> Dict[str, Any]:
        """Get summary of all aggregation operations."""
        return {
            "operations_count": len(self.aggregation_log),
            "operations": self.aggregation_log,
            "timestamp": datetime.utcnow().isoformat()
        }
