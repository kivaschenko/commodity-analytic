"""
Database Initialization Script
Creates warehouse schema and initial data.
"""

import logging
from sqlalchemy import create_engine, text
from config import settings

logger = logging.getLogger("[init_db]")


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
        schemas = get_all_schemas()

        for table_name, schema_sql in schemas.items():
            logger.info(f"Creating {table_name}...")
            for stmt in schema_sql.split(";"):
                stmt = stmt.strip()
                if stmt:
                    connection.execute(text(stmt))

        connection.commit()
        logger.info("Warehouse schema created successfully")
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
        views = get_all_views()

        for view_name, view_sql in views.items():
            logger.info(f"Creating view {view_name}...")
            for stmt in view_sql.split(";"):
                stmt = stmt.strip()
                if stmt:
                    connection.execute(text(stmt))

        connection.commit()
        logger.info("Analytical views created successfully")
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
        # Load currencies
        currencies = [
            (1, "USD", "US Dollar", "United States"),
            (2, "EUR", "Euro", "European Union"),
            (3, "UAH", "Ukrainian Hryvnia", "Ukraine"),
        ]

        logger.info("Loading currencies...")
        for currency in currencies:
            connection.execute(text(
                "INSERT INTO dim_currency (currency_key, currency_code, currency_name, country) VALUES (:currency_key, :currency_code, :currency_name, :country) ON CONFLICT DO NOTHING"
            ), {
                "currency_key": currency[0],
                "currency_code": currency[1],
                "currency_name": currency[2],
                "country": currency[3]
            })

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
            connection.execute(text(
                "INSERT INTO dim_commodity (commodity_key, commodity_name, commodity_type, category, unit, grade, origin_country) VALUES (:commodity_key, :commodity_name, :commodity_type, :category, :unit, :grade, :origin_country) ON CONFLICT DO NOTHING"
            ), {
                "commodity_key": commodity[0],
                "commodity_name": commodity[1],
                "commodity_type": commodity[2],
                "category": commodity[3],
                "unit": commodity[4],
                "grade": commodity[5],
                "origin_country": commodity[6]
            })

        # Load markets
        markets = [
            (1, "Chicago CBOT", "CBOT", "United States", "America/Chicago", "08:00-19:00"),
            (2, "ICE London", "ICE", "United Kingdom", "Europe/London", "09:00-19:00"),
            (3, "Kyiv Exchange", "ICEECX", "Ukraine", "Europe/Kyiv", "09:00-17:00"),
        ]

        logger.info("Loading markets...")
        for market in markets:
            connection.execute(text(
                "INSERT INTO dim_market (market_key, market_name, exchange, country, timezone, trading_hours) VALUES (:market_key, :market_name, :exchange, :country, :timezone, :trading_hours) ON CONFLICT DO NOTHING"
            ), {
                "market_key": market[0],
                "market_name": market[1],
                "exchange": market[2],
                "country": market[3],
                "timezone": market[4],
                "trading_hours": market[5]
            })

        # Load data sources
        sources = [
            (1, "yfinance", "yfinance", "prices", 4, "daily", "https://finance.yahoo.com"),
            (2, "graintradecomua", "graintradecomua", "prices", 4, "daily", "https://www.graintrade.com.ua"),
            (3, "currency", "currency_api", "exchange_rates", 5, "daily", "https://exchangeratesapi.io"),
            (4, "tripoli_land", "tripoli_land_parser", "grain_offers", 3, "daily", "https://tripoli.land"),
        ]

        logger.info("Loading data sources...")
        for source in sources:
            connection.execute(text(
                "INSERT INTO dim_source (source_key, source_name, parser_type, data_type, reliability_rating, update_frequency, api_endpoint) VALUES (:source_key, :source_name, :parser_type, :data_type, :reliability_rating, :update_frequency, :api_endpoint) ON CONFLICT DO NOTHING"
            ), {
                "source_key": source[0],
                "source_name": source[1],
                "parser_type": source[2],
                "data_type": source[3],
                "reliability_rating": source[4],
                "update_frequency": source[5],
                "api_endpoint": source[6]
            })

        connection.commit()
        logger.info("Reference data loaded successfully")
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
    
    url = settings.database_url
    engine = create_engine(url)
    connection = engine.connect()
    initialize_database(connection)
    connection.close()
    
    logger.info("Database initialization script loaded")
