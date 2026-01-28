"""
Warehouse Schema - Star schema for OLAP analytics.
Fact and dimension tables for commodity price analytics.
"""

# FACT TABLE
FACT_TABLE_SCHEMA = """
CREATE TABLE IF NOT EXISTS commodity_prices_fact (
    price_id BIGINT PRIMARY KEY,
    date_key INT,
    commodity_key INT,
    market_key INT,
    source_key INT,
    currency_key INT,
    
    -- Measures
    open_price DECIMAL(10, 2),
    close_price DECIMAL(10, 2),
    high_price DECIMAL(10, 2),
    low_price DECIMAL(10, 2),
    volume DECIMAL(15, 2),
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Foreign Keys
    FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
    FOREIGN KEY (commodity_key) REFERENCES dim_commodity(commodity_key),
    FOREIGN KEY (market_key) REFERENCES dim_market(market_key),
    FOREIGN KEY (source_key) REFERENCES dim_source(source_key),
    FOREIGN KEY (currency_key) REFERENCES dim_currency(currency_key)
);

CREATE INDEX idx_fact_date ON commodity_prices_fact(date_key);
CREATE INDEX idx_fact_commodity ON commodity_prices_fact(commodity_key);
CREATE INDEX idx_fact_market ON commodity_prices_fact(market_key);
CREATE INDEX idx_fact_source ON commodity_prices_fact(source_key);
"""

# DIMENSION: DATE
DIM_DATE_SCHEMA = """
CREATE TABLE IF NOT EXISTS dim_date (
    date_key INT PRIMARY KEY,
    calendar_date DATE UNIQUE NOT NULL,
    year INT,
    quarter INT,
    month INT,
    day INT,
    week_of_year INT,
    day_of_week VARCHAR(10),
    is_weekend BOOLEAN,
    is_holiday BOOLEAN,
    trading_status VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_date_calendar ON dim_date(calendar_date);
"""

# DIMENSION: COMMODITY
DIM_COMMODITY_SCHEMA = """
CREATE TABLE IF NOT EXISTS dim_commodity (
    commodity_key INT PRIMARY KEY,
    commodity_name VARCHAR(100) NOT NULL UNIQUE,
    commodity_type VARCHAR(50),
    category VARCHAR(50),
    unit VARCHAR(20),
    grade VARCHAR(50),
    origin_country VARCHAR(50),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    ended_at TIMESTAMP
);

CREATE INDEX idx_dim_commodity_name ON dim_commodity(commodity_name);
CREATE INDEX idx_dim_commodity_type ON dim_commodity(commodity_type);
"""

# DIMENSION: MARKET
DIM_MARKET_SCHEMA = """
CREATE TABLE IF NOT EXISTS dim_market (
    market_key INT PRIMARY KEY,
    market_name VARCHAR(100) NOT NULL UNIQUE,
    exchange VARCHAR(50),
    country VARCHAR(50),
    timezone VARCHAR(30),
    trading_hours VARCHAR(100),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    ended_at TIMESTAMP
);

CREATE INDEX idx_dim_market_name ON dim_market(market_name);
CREATE INDEX idx_dim_market_country ON dim_market(country);
"""

# DIMENSION: SOURCE
DIM_SOURCE_SCHEMA = """
CREATE TABLE IF NOT EXISTS dim_source (
    source_key INT PRIMARY KEY,
    source_name VARCHAR(100) NOT NULL UNIQUE,
    parser_type VARCHAR(50),
    data_type VARCHAR(50),
    reliability_rating INT,
    update_frequency VARCHAR(50),
    api_endpoint VARCHAR(500),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    ended_at TIMESTAMP
);

CREATE INDEX idx_dim_source_name ON dim_source(source_name);
CREATE INDEX idx_dim_source_parser ON dim_source(parser_type);
"""

# DIMENSION: CURRENCY
DIM_CURRENCY_SCHEMA = """
CREATE TABLE IF NOT EXISTS dim_currency (
    currency_key INT PRIMARY KEY,
    currency_code VARCHAR(3) NOT NULL UNIQUE,
    currency_name VARCHAR(50),
    country VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_currency_code ON dim_currency(currency_code);
"""

# AGGREGATE TABLES FOR PERFORMANCE
DAILY_PRICE_SUMMARY_SCHEMA = """
CREATE TABLE IF NOT EXISTS daily_price_summary (
    summary_id BIGINT PRIMARY KEY,
    date_key INT,
    commodity_key INT,
    market_key INT,
    
    open_price DECIMAL(10, 2),
    high_price DECIMAL(10, 2),
    low_price DECIMAL(10, 2),
    close_price DECIMAL(10, 2),
    volume DECIMAL(15, 2),
    
    price_change DECIMAL(10, 2),
    price_change_pct DECIMAL(5, 2),
    
    source_count INT,
    record_count INT,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_daily_summary_date ON daily_price_summary(date_key);
CREATE INDEX idx_daily_summary_commodity ON daily_price_summary(commodity_key);
"""

WEEKLY_COMMODITY_SUMMARY_SCHEMA = """
CREATE TABLE IF NOT EXISTS weekly_commodity_summary (
    summary_id BIGINT PRIMARY KEY,
    year INT,
    week_of_year INT,
    commodity_key INT,
    
    avg_price DECIMAL(10, 2),
    min_price DECIMAL(10, 2),
    max_price DECIMAL(10, 2),
    total_volume DECIMAL(15, 2),
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_weekly_summary_week ON weekly_commodity_summary(year, week_of_year);
CREATE INDEX idx_weekly_summary_commodity ON weekly_commodity_summary(commodity_key);
"""

MONTHLY_COMMODITY_SUMMARY_SCHEMA = """
CREATE TABLE IF NOT EXISTS monthly_commodity_summary (
    summary_id BIGINT PRIMARY KEY,
    year INT,
    month INT,
    commodity_key INT,
    
    avg_price DECIMAL(10, 2),
    min_price DECIMAL(10, 2),
    max_price DECIMAL(10, 2),
    total_volume DECIMAL(15, 2),
    price_volatility DECIMAL(5, 2),
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_monthly_summary_month ON monthly_commodity_summary(year, month);
CREATE INDEX idx_monthly_summary_commodity ON monthly_commodity_summary(commodity_key);
"""

# SCD TYPE 2: Track changes over time
DIMENSION_SCD2_TEMPLATE = """
-- Add to dimension tables for SCD Type 2 tracking
ALTER TABLE dim_commodity ADD COLUMN (
    effective_date DATE,
    end_date DATE,
    is_current BOOLEAN DEFAULT TRUE,
    version INT DEFAULT 1
);
"""

def get_all_schemas() -> dict:
    """Return all schema definitions."""
    return {
        "fact_table": FACT_TABLE_SCHEMA,
        "dim_date": DIM_DATE_SCHEMA,
        "dim_commodity": DIM_COMMODITY_SCHEMA,
        "dim_market": DIM_MARKET_SCHEMA,
        "dim_source": DIM_SOURCE_SCHEMA,
        "dim_currency": DIM_CURRENCY_SCHEMA,
        "daily_summary": DAILY_PRICE_SUMMARY_SCHEMA,
        "weekly_summary": WEEKLY_COMMODITY_SUMMARY_SCHEMA,
        "monthly_summary": MONTHLY_COMMODITY_SUMMARY_SCHEMA,
    }
