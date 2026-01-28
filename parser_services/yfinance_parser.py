"""
YFinance Parser - Simple commodity price fetcher for grain trading
Fetches futures contracts and ETFs from Yahoo Finance with RabbitMQ integration
"""

import yfinance as yf
from datetime import datetime
import pandas as pd
import json
import asyncio
import aio_pika
import os
import logging
from pathlib import Path
from dotenv import load_dotenv

from app.utils.rates import fetch_usd_to_uah

# Initialize logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Load environment variables
BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

# RabbitMQ Configuration
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", 5672))
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASS = os.getenv("RABBITMQ_PASS", "guest")
RABBITMQ_VHOST = os.getenv("RABBITMQ_VHOST", "/")
RABBITMQ_QUEUE = os.getenv("RABBITMQ_QUEUE", "message.events")


# --- COMMODITIES CONFIG ---
# Futures contracts and ETFs only
COMMODITIES = {
    # === FUTURES CONTRACTS (CBOT) - quoted in cents ===
    "Wheat Futures": {
        "ticker": "ZW=F",
        "unit": "bushel",
        "kg_per_unit": 27.2155,
        "cents_per_dollar": 100,
        "category": "futures",
        "description": "Пшениця (ф'ючерс CBOT)",
    },
    "Corn Futures": {
        "ticker": "ZC=F",
        "unit": "bushel",
        "kg_per_unit": 25.4012,
        "cents_per_dollar": 100,
        "category": "futures",
        "description": "Кукурудза (ф'ючерс CBOT)",
    },
    "Soybeans Futures": {
        "ticker": "ZS=F",
        "unit": "bushel",
        "kg_per_unit": 27.2155,
        "cents_per_dollar": 100,
        "category": "futures",
        "description": "Соя (ф'ючерс CBOT)",
    },
    "Oats Futures": {
        "ticker": "ZO=F",
        "unit": "bushel",
        "kg_per_unit": 14.5150,
        "cents_per_dollar": 100,
        "category": "futures",
        "description": "Овес (ф'ючерс CBOT)",
    },
    "Rough Rice Futures": {
        "ticker": "ZR=F",
        "unit": "cwt",
        "kg_per_unit": 45.359237,
        "cents_per_dollar": 100,
        "category": "futures",
        "description": "Рис (ф'ючерс CBOT)",
    },
    # === ETFs (Exchange Traded Funds) - quoted in dollars ===
    "Wheat ETF": {
        "ticker": "WEAT",
        "unit": "share",
        "kg_per_unit": None,
        "cents_per_dollar": 1,
        "category": "etf",
        "description": "Пшениця (ETF)",
    },
    "Corn ETF": {
        "ticker": "CORN",
        "unit": "share",
        "kg_per_unit": None,
        "cents_per_dollar": 1,
        "category": "etf",
        "description": "Кукурудза (ETF)",
    },
    "Soybeans ETF": {
        "ticker": "SOYB",
        "unit": "share",
        "kg_per_unit": None,
        "cents_per_dollar": 1,
        "category": "etf",
        "description": "Соя (ETF)",
    },
    "Agricultural Basket": {
        "ticker": "DBA",
        "unit": "share",
        "kg_per_unit": None,
        "cents_per_dollar": 1,
        "category": "etf",
        "description": "Аграрний кошик (ETF)",
    },
    "Sugar ETF": {
        "ticker": "CANE",
        "unit": "share",
        "kg_per_unit": None,
        "cents_per_dollar": 1,
        "category": "etf",
        "description": "Цукор (ETF)",
    },
    "Coffee ETF": {
        "ticker": "JO",
        "unit": "share",
        "kg_per_unit": None,
        "cents_per_dollar": 1,
        "category": "etf",
        "description": "Кава (ETF)",
    },
}


# --- HELPER FUNCTIONS ---


def fetch_price_yf(ticker: str):
    """
    Fetch last close price from Yahoo Finance.
    Handles anomalies in rice futures and other data inconsistencies.
    """
    try:
        t = yf.Ticker(ticker)
        hist = t.history(period="5d")  # Get more days to detect anomalies
        if hist.empty:
            logger.warning(f"No data available for {ticker}")
            return None

        # Special handling for rough rice futures (ZR=F) due to pricing anomalies
        if ticker == "ZR=F" and len(hist) > 1:
            recent_prices = hist["Close"].tail(5)
            latest_price = recent_prices.iloc[-1]
            # If latest price is dramatically different from recent average, use previous value
            if len(recent_prices) > 1:
                avg_previous = recent_prices.iloc[:-1].mean()
                if (
                    abs(latest_price - avg_previous) / avg_previous > 0.5
                ):  # 50% difference threshold
                    logger.warning(
                        f"{ticker} latest price {latest_price:.2f} differs significantly "
                        f"from recent average {avg_previous:.2f}, using previous price"
                    )
                    return float(recent_prices.iloc[-2])

        return float(hist["Close"].iloc[-1])
    except Exception as e:
        logger.error(f"Error fetching {ticker}: {e}")
        return None


def convert_to_usd_per_ton(price: float, kg_per_unit: float):
    """
    Convert price quoted per unit (e.g., per bushel or per cwt) into USD/tonne.

    Args:
        price: Price in USD per unit
        kg_per_unit: How many kg in 1 quoted unit

    Returns:
        Price in USD per metric ton (1000 kg), or None if conversion not possible
    """
    if price is None or kg_per_unit is None:
        return None
    # 1 tonne = 1000 kg, factor = 1000 / kg_per_unit
    factor = 1000.0 / kg_per_unit
    return price * factor


def get_commodity_prices(usd_to_uah: float) -> pd.DataFrame:
    """
    Fetch and process all commodity prices from Yahoo Finance.

    Args:
        usd_to_uah: Current USD to UAH exchange rate

    Returns:
        DataFrame with commodity data including conversions to USD/ton and UAH/ton
    """
    rows = []

    for name, cfg in COMMODITIES.items():
        try:
            price = fetch_price_yf(cfg["ticker"])

            if price is None:
                logger.warning(f"No price data for {name} ({cfg['ticker']})")
                rows.append(
                    {
                        "name": name,
                        "ticker": cfg["ticker"],
                        "category": cfg["category"],
                        "raw_price": None,
                        "price_in_dollars": None,
                        "unit": cfg["unit"],
                        "usd_per_ton": None,
                        "uah_per_ton": None,
                        "usd_per_share": None,
                        "uah_per_share": None,
                        "description": cfg["description"],
                        "note": "дані недоступні",
                    }
                )
                continue

            # Convert from cents to dollars if needed
            price_in_dollars = price / cfg.get("cents_per_dollar", 1)

            # Process based on unit type
            if cfg["unit"] == "share":
                # ETF or stock: keep share price
                usd_per_share = price_in_dollars
                uah_per_share = usd_per_share * usd_to_uah

                rows.append(
                    {
                        "name": name,
                        "ticker": cfg["ticker"],
                        "category": cfg["category"],
                        "raw_price": price,
                        "price_in_dollars": price_in_dollars,
                        "unit": cfg["unit"],
                        "usd_per_ton": None,
                        "uah_per_ton": None,
                        "usd_per_share": usd_per_share,
                        "uah_per_share": uah_per_share,
                        "description": cfg["description"],
                        "note": "ETF/акція"
                        if cfg["category"] == "etf"
                        else "акція компанії",
                    }
                )
            else:
                # Futures: convert to USD/ton and UAH/ton
                usd_per_ton = convert_to_usd_per_ton(
                    price_in_dollars, cfg.get("kg_per_unit")
                )
                uah_per_ton = usd_per_ton * usd_to_uah if usd_per_ton else None

                rows.append(
                    {
                        "name": name,
                        "ticker": cfg["ticker"],
                        "category": cfg["category"],
                        "raw_price": price,
                        "price_in_dollars": price_in_dollars,
                        "unit": cfg["unit"],
                        "usd_per_ton": usd_per_ton,
                        "uah_per_ton": uah_per_ton,
                        "usd_per_share": None,
                        "uah_per_share": None,
                        "description": cfg["description"],
                        "note": "ф'ючерсний контракт",
                    }
                )

        except Exception as e:
            logger.error(f"Error processing {name}: {e}")
            continue

    df = pd.DataFrame(rows)
    return df


def format_telegram_daily_report(df: pd.DataFrame, usd_to_uah: float) -> str:
    """
    Format daily commodity report for Telegram.
    Includes futures and ETFs only.
    """
    now = datetime.now()
    current_date = now.strftime("%d.%m.%Y")
    current_time = now.strftime("%H:%M")

    # Header
    message = f"📊 <b>Щоденний огляд аграрного ринку</b> — {current_date}\n"
    message += f"💱 Курс USD→UAH: {usd_to_uah:.2f}\n\n"

    # Group by categories
    futures_df = df[df["category"] == "futures"]
    etf_df = df[df["category"] == "etf"]

    # Futures section
    if not futures_df.empty:
        message += "🌾 <b>Ф'ючерсні контракти (CBOT):</b>\n"
        for _, row in futures_df.iterrows():
            if pd.notna(row["price_in_dollars"]) and pd.notna(row["usd_per_ton"]):
                message += (
                    f"• {row['description']}: "
                    f"{row['price_in_dollars']:.2f} USD/{row['unit']} "
                    f"≈ {row['usd_per_ton']:.2f} USD/т "
                    f"≈ {row['uah_per_ton']:.0f} ₴/т\n"
                )
            else:
                message += f"• {row['description']}: {row['note']}\n"
        message += "\n"

    # ETFs section
    if not etf_df.empty:
        message += "📈 <b>Товарні ETF:</b>\n"
        for _, row in etf_df.iterrows():
            if pd.notna(row["usd_per_share"]):
                message += (
                    f"• {row['description']}: "
                    f"${row['usd_per_share']:.2f} "
                    f"({row['uah_per_share']:.0f} ₴)\n"
                )
            else:
                message += f"• {row['description']}: {row['note']}\n"
        message += "\n"

    # Footer
    message += "📝 <b>Дані отримані з фондових бірж у реальному часі</b>\n"
    message += f"🕐 Оновлено: {current_time} (UTC timezone) {current_date}\n"
    message += "🔎 Джерела: Yahoo Finance (CBOT, NYSE, NASDAQ)"

    return message


def format_telegram_weekly_digest(df: pd.DataFrame, usd_to_uah: float) -> str:
    """
    Format weekly comprehensive digest for Telegram.
    Includes futures and ETFs with detailed explanations for traders.
    """
    now = datetime.now()
    current_date = now.strftime("%d.%m.%Y")
    current_time = now.strftime("%H:%M")

    # Header
    message = f"📆 <b>Тижневий дайджест зернового ринку</b> — {current_date}\n"
    message += f"💱 USD→UAH: {usd_to_uah:.2f}\n\n"

    # Futures section (most important for grain trading)
    futures_df = df[df["category"] == "futures"]
    if not futures_df.empty:
        message += "🌍 <b>Світові біржові котирування (ф'ючерси CBOT):</b>\n\n"
        for _, row in futures_df.iterrows():
            if pd.notna(row["price_in_dollars"]) and pd.notna(row["usd_per_ton"]):
                desc = row["description"].replace(" (ф'ючерс CBOT)", "")
                message += (
                    f"• {desc}\n"
                    f"  {row['price_in_dollars']:.2f} USD/{row['unit']} | "
                    f"{row['usd_per_ton']:.2f} USD/т | "
                    f"{row['uah_per_ton']:.0f} ₴/т\n"
                )
            else:
                message += f"• {row['description']}: {row['note']}\n"
        message += "\n"

    # ETFs section
    etf_df = df[df["category"] == "etf"]
    if not etf_df.empty:
        message += "📊 <b>ETF (біржові фонди):</b>\n"
        for _, row in etf_df.iterrows():
            if pd.notna(row["usd_per_share"]):
                message += f"• {row['description']}: ${row['usd_per_share']:.2f} ({row['uah_per_share']:.0f} ₴)\n"
        message += "\n"

    # Explanations for traders
    message += "ℹ️ <b>Пояснення для трейдерів:</b>\n"
    message += "• Ф'ючерси CBOT котируються в центах за бушель або cwt (100 фунтів)\n"
    message += "• Конверсія: бушель→тонна залежить від культури (різна вага)\n"
    message += "• ETF — фінансовий інструмент, не пряма ціна фізичного товару\n\n"

    # Footer
    message += f"🕐 Оновлено: {current_time} (UTC timezone) {current_date}\n"
    message += "🔎 Джерела: Yahoo Finance (ф'ючерси CBOT, ETF)"

    return message


async def publish_to_rabbitmq(message_data: dict):
    """Publish message to RabbitMQ for notifications service"""
    try:
        connection = await aio_pika.connect_robust(
            host=RABBITMQ_HOST,
            port=RABBITMQ_PORT,
            login=RABBITMQ_USER,
            password=RABBITMQ_PASS,
            virtualhost=RABBITMQ_VHOST,
        )

        channel = await connection.channel()

        # Declare queue for message events
        await channel.declare_queue(RABBITMQ_QUEUE, durable=True)

        # Publish message
        await channel.default_exchange.publish(
            aio_pika.Message(
                body=json.dumps(message_data, ensure_ascii=False).encode("utf-8"),
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                content_type="application/json",
                content_encoding="utf-8",
            ),
            routing_key=RABBITMQ_QUEUE,
        )

        logger.info(f"Message published to RabbitMQ: {message_data['type']}")
        await connection.close()

    except Exception as e:
        logger.error(f"Failed to publish to RabbitMQ: {e}")
        raise


async def generate_daily_report():
    """Generate and publish daily commodity report"""
    try:
        logger.info("Generating daily commodity report...")

        # Get exchange rate
        usd_to_uah = fetch_usd_to_uah()

        # Get commodity prices
        df = get_commodity_prices(usd_to_uah)

        if df.empty:
            logger.warning("No commodity data available")
            return

        # Format Telegram message
        telegram_message = format_telegram_daily_report(df, usd_to_uah)

        # Prepare message for RabbitMQ
        message_data = {
            "type": "commodity_prices_daily",
            "timestamp": datetime.now().isoformat(),
            "data": {
                "telegram_message": telegram_message,
                "usd_uah_rate": usd_to_uah,
                "commodities": df.to_dict("records"),
            },
            "destination": "telegram_channel",
        }

        # Publish to RabbitMQ
        await publish_to_rabbitmq(message_data)

        logger.info("Daily report published successfully")
        return telegram_message

    except Exception as e:
        logger.error(f"Error generating daily report: {e}")
        raise


async def generate_weekly_digest():
    """Generate and publish weekly comprehensive digest"""
    try:
        logger.info("Generating weekly commodity digest...")

        # Get exchange rate
        usd_to_uah = fetch_usd_to_uah()

        # Get commodity prices
        df = get_commodity_prices(usd_to_uah)

        if df.empty:
            logger.warning("No commodity data available")
            return

        # Format Telegram message
        telegram_message = format_telegram_weekly_digest(df, usd_to_uah)

        # Save to markdown file
        try:
            output_file = Path("digest.md")
            output_file.write_text(telegram_message, encoding="utf-8")
            logger.info(f"Saved digest to {output_file}")
        except Exception as e:
            logger.warning(f"Failed to save digest.md: {e}")

        # Prepare message for RabbitMQ
        message_data = {
            "type": "commodity_prices_weekly",
            "timestamp": datetime.now().isoformat(),
            "data": {
                "telegram_message": telegram_message,
                "usd_uah_rate": usd_to_uah,
                "commodities": df.to_dict("records"),
            },
            "destination": "telegram_channel",
        }

        # Publish to RabbitMQ
        await publish_to_rabbitmq(message_data)

        logger.info("Weekly digest published successfully")
        return telegram_message

    except Exception as e:
        logger.error(f"Error generating weekly digest: {e}")
        raise


async def main():
    """
    Main function - runs both daily report and weekly digest
    Use command line arguments to specify which one to run
    """
    import sys

    report_type = sys.argv[1] if len(sys.argv) > 1 else "daily"

    try:
        if report_type == "weekly":
            logger.info("Running weekly digest mode...")
            message = await generate_weekly_digest()
            print("\n--- Weekly Digest Preview ---\n")
            print(message)
        else:
            logger.info("Running daily report mode...")
            message = await generate_daily_report()
            print("\n--- Daily Report Preview ---\n")
            print(message)

        print("\n✅ Data sent to RabbitMQ successfully!")

    except Exception as e:
        logger.error(f"Script failed: {e}")
        raise


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Script interrupted by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        exit(1)
