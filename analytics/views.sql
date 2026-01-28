"""
Analytical Views - SQL views for common analytics queries.
"""

# Price trend views
PRICE_TRENDS_VIEW = """
CREATE OR REPLACE VIEW v_price_trends AS
SELECT 
    cpf.date_key,
    dc.commodity_name,
    dm.market_name,
    cpf.close_price,
    cpf.volume,
    LAG(cpf.close_price) OVER (PARTITION BY dc.commodity_key ORDER BY cpf.date_key) as prev_price,
    cpf.close_price - LAG(cpf.close_price) OVER (PARTITION BY dc.commodity_key ORDER BY cpf.date_key) as price_change,
    ROUND(100 * (cpf.close_price - LAG(cpf.close_price) OVER (PARTITION BY dc.commodity_key ORDER BY cpf.date_key)) 
          / LAG(cpf.close_price) OVER (PARTITION BY dc.commodity_key ORDER BY cpf.date_key), 2) as price_change_pct
FROM commodity_prices_fact cpf
JOIN dim_commodity dc ON cpf.commodity_key = dc.commodity_key
JOIN dim_market dm ON cpf.market_key = dm.market_key
JOIN dim_date dd ON cpf.date_key = dd.date_key
ORDER BY dc.commodity_name, dd.calendar_date DESC;
"""

# Volatility view
VOLATILITY_VIEW = """
CREATE OR REPLACE VIEW v_commodity_volatility AS
SELECT 
    dc.commodity_key,
    dc.commodity_name,
    dd.year,
    dd.month,
    STDDEV(cpf.close_price) as price_volatility,
    AVG(cpf.volume) as avg_volume,
    MAX(cpf.high_price) - MIN(cpf.low_price) as price_range
FROM commodity_prices_fact cpf
JOIN dim_commodity dc ON cpf.commodity_key = dc.commodity_key
JOIN dim_date dd ON cpf.date_key = dd.date_key
GROUP BY dc.commodity_key, dc.commodity_name, dd.year, dd.month
ORDER BY dc.commodity_name, dd.year DESC, dd.month DESC;
"""

# Source comparison view
SOURCE_COMPARISON_VIEW = """
CREATE OR REPLACE VIEW v_source_price_comparison AS
SELECT 
    dd.calendar_date,
    dc.commodity_name,
    ds.source_name,
    cpf.close_price,
    cpf.volume,
    COUNT(*) OVER (PARTITION BY dd.calendar_date, dc.commodity_key) as sources_count
FROM commodity_prices_fact cpf
JOIN dim_date dd ON cpf.date_key = dd.date_key
JOIN dim_commodity dc ON cpf.commodity_key = dc.commodity_key
JOIN dim_source ds ON cpf.source_key = ds.source_key
ORDER BY dd.calendar_date DESC, dc.commodity_name, ds.source_name;
"""

# Cross-commodity correlation view
CORRELATION_VIEW = """
CREATE OR REPLACE VIEW v_commodity_correlation AS
SELECT 
    c1.commodity_name as commodity_1,
    c2.commodity_name as commodity_2,
    CORR(p1.close_price, p2.close_price) as price_correlation,
    COUNT(*) as overlap_days
FROM commodity_prices_fact p1
JOIN commodity_prices_fact p2 
    ON p1.date_key = p2.date_key 
    AND p1.market_key = p2.market_key
    AND p1.commodity_key < p2.commodity_key
JOIN dim_commodity c1 ON p1.commodity_key = c1.commodity_key
JOIN dim_commodity c2 ON p2.commodity_key = c2.commodity_key
GROUP BY c1.commodity_name, c2.commodity_name
ORDER BY commodity_1, commodity_2;
"""

# YoY comparison view
YOY_COMPARISON_VIEW = """
CREATE OR REPLACE VIEW v_yoy_price_comparison AS
SELECT 
    dd_current.month,
    dc.commodity_name,
    dd_current.year as current_year,
    AVG(cpf_current.close_price) as current_year_avg_price,
    dd_previous.year as previous_year,
    AVG(cpf_previous.close_price) as previous_year_avg_price,
    ROUND(100 * (AVG(cpf_current.close_price) - AVG(cpf_previous.close_price)) 
          / AVG(cpf_previous.close_price), 2) as yoy_change_pct
FROM commodity_prices_fact cpf_current
JOIN commodity_prices_fact cpf_previous 
    ON cpf_current.commodity_key = cpf_previous.commodity_key
    AND EXTRACT(MONTH FROM cpf_current.date_key) = EXTRACT(MONTH FROM cpf_previous.date_key)
    AND EXTRACT(YEAR FROM cpf_current.date_key) = EXTRACT(YEAR FROM cpf_previous.date_key) + 1
JOIN dim_commodity dc ON cpf_current.commodity_key = dc.commodity_key
JOIN dim_date dd_current ON cpf_current.date_key = dd_current.date_key
JOIN dim_date dd_previous ON cpf_previous.date_key = dd_previous.date_key
GROUP BY dd_current.month, dc.commodity_name, dd_current.year, dd_previous.year
ORDER BY dc.commodity_name, current_year DESC;
"""

# Trading volume analysis view
VOLUME_ANALYSIS_VIEW = """
CREATE OR REPLACE VIEW v_volume_analysis AS
SELECT 
    dd.calendar_date,
    dc.commodity_name,
    dm.market_name,
    cpf.volume,
    AVG(cpf.volume) OVER (PARTITION BY dc.commodity_key ORDER BY dd.calendar_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) as volume_30d_avg,
    cpf.volume / AVG(cpf.volume) OVER (PARTITION BY dc.commodity_key ORDER BY dd.calendar_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) as volume_ratio_to_avg
FROM commodity_prices_fact cpf
JOIN dim_date dd ON cpf.date_key = dd.date_key
JOIN dim_commodity dc ON cpf.commodity_key = dc.commodity_key
JOIN dim_market dm ON cpf.market_key = dm.market_key
ORDER BY dc.commodity_name, dd.calendar_date DESC;
"""

def get_all_views() -> dict:
    """Return all analytical views."""
    return {
        "price_trends": PRICE_TRENDS_VIEW,
        "volatility": VOLATILITY_VIEW,
        "source_comparison": SOURCE_COMPARISON_VIEW,
        "correlation": CORRELATION_VIEW,
        "yoy_comparison": YOY_COMPARISON_VIEW,
        "volume_analysis": VOLUME_ANALYSIS_VIEW,
    }
