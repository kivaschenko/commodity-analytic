"""
Database Initialization Script
Creates warehouse schema and initial data.
"""

import logging
from typing import Optional

logger = logging.getLogger(__name__)


def create_warehouse_schema(connection=None) -> bool:
    """
    Create warehouse schema with all tables.
    
    Args:
        connection: Database connection
    
    Returns:
        True if successful
    """
    if connection is None:
        logger.error("No database connection provided")
        return False

    from warehouse.schema import get_all_schemas

    try:
        cursor = connection.cursor()
        schemas = get_all_schemas()

        for table_name, schema_sql in schemas.items():
            logger.info(f"Creating {table_name}...")
            cursor.execute(schema_sql)

        connection.commit()
        logger.info("Warehouse schema created successfully")
        cursor.close()
        return True

    except Exception as e:
        logger.error(f"Error creating schema: {e}")
        connection.rollback()
        return False


def create_analytical_views(connection=None) -> bool:
    """
    Create analytical views.
    
    Args:
        connection: Database connection
    
    Returns:
        True if successful
    """
    if connection is None:
        logger.error("No database connection provided")
        return False

    from analytics.views import get_all_views

    try:
        cursor = connection.cursor()
        views = get_all_views()

        for view_name, view_sql in views.items():
            logger.info(f"Creating view {view_name}...")
            cursor.execute(view_sql)

        connection.commit()
        logger.info("Analytical views created successfully")
        cursor.close()
        return True

    except Exception as e:
        logger.error(f"Error creating views: {e}")
        connection.rollback()
        return False


def load_reference_data(connection=None) -> bool:
    """
    Load reference/master data into dimension tables.
    
    Args:
        connection: Database connection
    
    Returns:
        True if successful
    """
    if connection is None:
        logger.error("No database connection provided")
        return False

    try:
        cursor = connection.cursor()

        # Load currencies
        currencies = [
            (1, "USD", "US Dollar", "United States"),
            (2, "EUR", "Euro", "European Union"),
            (3, "UAH", "Ukrainian Hryvnia", "Ukraine"),
        ]

        logger.info("Loading currencies...")
        for currency in currencies:
            cursor.execute(
                "INSERT INTO dim_currency (currency_key, currency_code, currency_name, country) VALUES (%s, %s, %s, %s)",
                currency
            )

        # Load commodities
        commodities = [
            (1, "Wheat", "grain", "cereals", "ton", None, "Ukraine"),
            (2, "Corn", "grain", "cereals", "ton", None, "Ukraine"),
            (3, "Soybeans", "oil_crop", "legumes", "ton", None, "Ukraine"),
            (4, "Barley", "grain", "cereals", "ton", None, "Ukraine"),
            (5, "Rapeseed", "oil_crop", "oil_seeds", "ton", None, "Ukraine"),
        ]

        logger.info("Loading commodities...")
        for commodity in commodities:
            cursor.execute(
                "INSERT INTO dim_commodity (commodity_key, commodity_name, commodity_type, category, unit, grade, origin_country) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                commodity
            )

        # Load markets
        markets = [
            (1, "Chicago CBOT", "CBOT", "United States", "America/Chicago", "08:00-19:00"),
            (2, "ICE London", "ICE", "United Kingdom", "Europe/London", "09:00-19:00"),
            (3, "Kyiv Exchange", "ICEECX", "Ukraine", "Europe/Kyiv", "09:00-17:00"),
        ]

        logger.info("Loading markets...")
        for market in markets:
            cursor.execute(
                "INSERT INTO dim_market (market_key, market_name, exchange, country, timezone, trading_hours) VALUES (%s, %s, %s, %s, %s, %s)",
                market
            )

        # Load data sources
        sources = [
            (1, "yfinance", "yfinance", "prices", 4, "daily", "https://finance.yahoo.com"),
            (2, "investingcom", "investingcom", "prices", 3, "daily", None),
            (3, "graintradecomua", "graintradecomua", "prices", 4, "daily", "https://www.graintrade.com.ua"),
            (4, "apkinform", "apkinform", "prices", 4, "daily", "https://www.apkinform.com"),
        ]

        logger.info("Loading data sources...")
        for source in sources:
            cursor.execute(
                "INSERT INTO dim_source (source_key, source_name, parser_type, data_type, reliability_rating, update_frequency, api_endpoint) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                source
            )

        connection.commit()
        logger.info("Reference data loaded successfully")
        cursor.close()
        return True

    except Exception as e:
        logger.error(f"Error loading reference data: {e}")
        connection.rollback()
        return False


def initialize_database(connection=None) -> bool:
    """
    Complete database initialization.
    
    Args:
        connection: Database connection
    
    Returns:
        True if successful
    """
    logger.info("Starting database initialization...")

    if not create_warehouse_schema(connection):
        return False

    if not create_analytical_views(connection):
        return False

    if not load_reference_data(connection):
        return False

    logger.info("Database initialization completed successfully!")
    return True


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    # TODO: Create database connection and call initialize_database()
    # from sqlalchemy import create_engine
    # engine = create_engine(settings.database_url)
    # connection = engine.connect()
    # initialize_database(connection)
    # connection.close()
    
    logger.info("Database initialization script loaded")
