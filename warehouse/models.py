"""
Data Models - ORM/dataclass models for warehouse entities.
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional


@dataclass
class DimDate:
    """Date dimension record."""
    date_key: int
    calendar_date: str  # YYYY-MM-DD
    year: int
    quarter: int
    month: int
    day: int
    week_of_year: int
    day_of_week: str
    is_weekend: bool
    is_holiday: bool = False
    trading_status: str = "active"
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass
class DimCommodity:
    """Commodity dimension record."""
    commodity_key: int
    commodity_name: str
    commodity_type: str
    category: str
    unit: str
    grade: Optional[str] = None
    origin_country: Optional[str] = None
    is_active: bool = True
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    ended_at: Optional[datetime] = None


@dataclass
class DimMarket:
    """Market dimension record."""
    market_key: int
    market_name: str
    exchange: str
    country: str
    timezone: str
    trading_hours: str
    is_active: bool = True
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    ended_at: Optional[datetime] = None


@dataclass
class DimSource:
    """Data source dimension record."""
    source_key: int
    source_name: str
    parser_type: str
    data_type: str
    reliability_rating: int  # 1-5
    update_frequency: str
    api_endpoint: Optional[str] = None
    is_active: bool = True
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    ended_at: Optional[datetime] = None


@dataclass
class DimCurrency:
    """Currency dimension record."""
    currency_key: int
    currency_code: str  # ISO 4217
    currency_name: str
    country: str
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass
class DimExchangeRate:
    """Exchange rate dimension record."""
    exchange_rate_key: int
    date_key: int
    base_currency: str  # ISO 4217
    quote_currency: str  # ISO 4217
    exchange_rate: float
    source: Optional[str] = None
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass
class CommodityPriceFact:
    """Fact table for commodity prices."""
    price_id: int
    date_key: int
    commodity_key: int
    market_key: int
    source_key: int
    currency_key: int
    
    # Measures
    open_price: float
    close_price: float
    high_price: float
    low_price: float
    volume: float
    
    # Classification
    price_type: str = "spot"  # "futures_close", "spot", "bid", "fx_rate"
    delivery_term: Optional[str] = None  # "FCA", "CPT", "CIF", "FOB"
    
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass
class DailyPriceSummary:
    """Daily price aggregate."""
    summary_id: int
    date_key: int
    commodity_key: int
    market_key: int
    
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    volume: float
    
    price_change: Optional[float] = None
    price_change_pct: Optional[float] = None
    
    source_count: int = 0
    record_count: int = 0
    
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass
class WeeklyCommoditySummary:
    """Weekly commodity aggregate."""
    summary_id: int
    year: int
    week_of_year: int
    commodity_key: int
    
    avg_price: float
    min_price: float
    max_price: float
    total_volume: float
    
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass
class MonthlyCommoditySummary:
    """Monthly commodity aggregate."""
    summary_id: int
    year: int
    month: int
    commodity_key: int
    
    avg_price: float
    min_price: float
    max_price: float
    total_volume: float
    price_volatility: Optional[float] = None
    
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
