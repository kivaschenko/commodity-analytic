"""
Feature Engineering for ML Models.
Creates features for time-series forecasting and classification tasks.
"""

import logging
from typing import List, Dict, Any, Tuple
import numpy as np
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class FeatureEngineer:
    """
    Creates features for ML prediction models:
    - Lag features (previous prices)
    - Rolling statistics (moving averages, volatility)
    - Momentum indicators
    - Seasonal decomposition
    - Correlation features
    """

    def __init__(self, lookback_window: int = 30):
        """
        Args:
            lookback_window: Number of historical periods for lag features
        """
        self.lookback_window = lookback_window
        self.feature_log = []

    def create_lag_features(self, data: List[Dict],
                           price_column: str = "close_price",
                           lags: List[int] = None) -> List[Dict]:
        """
        Create lagged price features (previous N days).
        
        Args:
            data: Time-series data sorted by date
            price_column: Column with price values
            lags: List of lag periods (default: [1, 7, 30])
        
        Returns:
            Data with lag features added
        """
        if lags is None:
            lags = [1, 7, 30]

        enriched = []
        prices = [record.get(price_column) for record in data]

        for idx, record in enumerate(data):
            enriched_record = record.copy()

            for lag in lags:
                if idx >= lag:
                    enriched_record[f"price_lag_{lag}d"] = prices[idx - lag]
                else:
                    enriched_record[f"price_lag_{lag}d"] = None

            enriched.append(enriched_record)

        self.feature_log.append({
            "operation": "create_lag_features",
            "lags": lags,
            "rows": len(enriched)
        })

        return enriched

    def create_rolling_features(self, data: List[Dict],
                               price_column: str = "close_price",
                               windows: List[int] = None) -> List[Dict]:
        """
        Create rolling window statistics (moving averages, volatility).
        
        Args:
            data: Time-series data
            price_column: Column with price values
            windows: Window sizes in days (default: [7, 30, 90])
        
        Returns:
            Data with rolling features
        """
        if windows is None:
            windows = [7, 30, 90]

        enriched = []
        prices = [record.get(price_column) for record in data 
                 if price_column in record and record[price_column] is not None]

        for idx, record in enumerate(data):
            enriched_record = record.copy()

            for window in windows:
                if idx >= window:
                    window_prices = prices[idx - window:idx]
                    
                    if window_prices:
                        enriched_record[f"price_ma_{window}d"] = np.mean(window_prices)
                        enriched_record[f"price_std_{window}d"] = np.std(window_prices)
                        enriched_record[f"price_min_{window}d"] = np.min(window_prices)
                        enriched_record[f"price_max_{window}d"] = np.max(window_prices)
                else:
                    enriched_record[f"price_ma_{window}d"] = None
                    enriched_record[f"price_std_{window}d"] = None
                    enriched_record[f"price_min_{window}d"] = None
                    enriched_record[f"price_max_{window}d"] = None

            enriched.append(enriched_record)

        self.feature_log.append({
            "operation": "create_rolling_features",
            "windows": windows,
            "rows": len(enriched)
        })

        return enriched

    def create_momentum_features(self, data: List[Dict],
                                price_column: str = "close_price") -> List[Dict]:
        """
        Create momentum indicators (RSI, MACD, rate of change).
        
        Args:
            data: Time-series data
            price_column: Column with price values
        
        Returns:
            Data with momentum features
        """
        enriched = []
        prices = [record.get(price_column) for record in data]

        for idx, record in enumerate(data):
            enriched_record = record.copy()

            # Rate of change (ROC) - 7 day
            if idx >= 7:
                roc_7 = ((prices[idx] - prices[idx - 7]) / prices[idx - 7]) * 100
                enriched_record["momentum_roc_7d"] = roc_7
            else:
                enriched_record["momentum_roc_7d"] = None

            # Rate of change (ROC) - 30 day
            if idx >= 30:
                roc_30 = ((prices[idx] - prices[idx - 30]) / prices[idx - 30]) * 100
                enriched_record["momentum_roc_30d"] = roc_30
            else:
                enriched_record["momentum_roc_30d"] = None

            enriched.append(enriched_record)

        self.feature_log.append({
            "operation": "create_momentum_features",
            "rows": len(enriched)
        })

        return enriched

    def create_seasonal_features(self, data: List[Dict],
                                date_column: str = "date") -> List[Dict]:
        """
        Create seasonal and cyclical features.
        
        Args:
            data: Time-series data with date column
            date_column: Column with date values
        
        Returns:
            Data with seasonal features
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

                    # Month encoding (sin/cos for cyclical)
                    month_sin = np.sin(2 * np.pi * (date_obj.month) / 12)
                    month_cos = np.cos(2 * np.pi * (date_obj.month) / 12)

                    enriched_record["seasonal_month_sin"] = month_sin
                    enriched_record["seasonal_month_cos"] = month_cos
                    enriched_record["seasonal_quarter"] = (date_obj.month - 1) // 3 + 1
                    enriched_record["seasonal_is_harvest"] = date_obj.month in [9, 10, 11]

                except Exception as e:
                    logger.warning(f"Failed to create seasonal features: {e}")

            enriched.append(enriched_record)

        self.feature_log.append({
            "operation": "create_seasonal_features",
            "rows": len(enriched)
        })

        return enriched

    def create_market_structure_features(self, data: List[Dict]) -> List[Dict]:
        """
        Create features based on market structure.
        
        Args:
            data: Price data with OHLCV columns
        
        Returns:
            Data with market structure features
        """
        enriched = []

        for record in data:
            enriched_record = record.copy()

            # Typical price
            if all(col in record for col in ["high_price", "low_price", "close_price"]):
                typical_price = (record["high_price"] + record["low_price"] + record["close_price"]) / 3
                enriched_record["market_typical_price"] = typical_price

            # Price range
            if all(col in record for col in ["high_price", "low_price"]):
                price_range = record["high_price"] - record["low_price"]
                enriched_record["market_price_range"] = price_range

            # Close position (relative to day's range)
            if all(col in record for col in ["open_price", "close_price", "high_price", "low_price"]):
                range_val = record["high_price"] - record["low_price"]
                if range_val > 0:
                    close_position = (record["close_price"] - record["low_price"]) / range_val
                    enriched_record["market_close_position"] = close_position

            enriched.append(enriched_record)

        self.feature_log.append({
            "operation": "create_market_structure_features",
            "rows": len(enriched)
        })

        return enriched

    def get_feature_report(self) -> Dict[str, Any]:
        """Get summary of all feature engineering operations."""
        return {
            "operations_count": len(self.feature_log),
            "operations": self.feature_log,
            "timestamp": datetime.utcnow().isoformat()
        }
