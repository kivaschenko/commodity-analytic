# Data Dictionary - Commodity Analytics ETL Pipeline

## Overview

This document describes the data structures, schemas, and transformations used in the commodity analytics ETL pipeline. It covers raw source data, transformed data, and warehouse schema.

## 1. Source Data Structures

### 1.1 Currency Parser (NBU)

**Source**: National Bank of Ukraine API
**Format**: JSON Array
**Update Frequency**: Daily

| Field | Type | Description | Example | Required |
|-------|------|-------------|---------|----------|
| _staging_timestamp | string | ISO timestamp when staged | "2026-04-07T08:53:40.828282" | Yes |
| _source | string | Source identifier | "currency" | Yes |
| _staging_id | string | Unique staging identifier | "currency_1775541220.828282" | Yes |
| provider | string | Data provider | "NBU" | Yes |
| source_endpoint | string | API endpoint URL | "https://bank.gov.ua/..." | Yes |
| base_currency | string | Base currency code (ISO 4217) | "USD" | Yes |
| quote_currency | string | Quote currency code | "UAH" | Yes |
| rate | float | Exchange rate | 36.5 | Yes |
| buy_rate | float/null | Buy rate (if available) | null | No |
| sell_rate | float/null | Sell rate (if available) | null | No |
| rate_type | string | Rate type | "official" | Yes |
| source_timestamp | string | Source data timestamp | "07.04.2026" | Yes |
| note | string | Processing note | "ok" | Yes |
| extracted_at | string | Extraction timestamp | "2026-04-07T08:53:39.867602+00:00" | Yes |

### 1.2 YFinance Parser (Yahoo Finance)

**Source**: Yahoo Finance API
**Format**: JSON Array
**Update Frequency**: Daily (market hours)

| Field | Type | Description | Example | Required |
|-------|------|-------------|---------|----------|
| _staging_timestamp | string | ISO timestamp when staged | "2026-04-07T07:51:20.901077" | Yes |
| _source | string | Source identifier | "yfinance" | Yes |
| _staging_id | string | Unique staging identifier | "yfinance_1775537480.901077" | Yes |
| name | string | Commodity name | "Wheat Futures" | Yes |
| ticker | string | Trading symbol | "ZW=F" | Yes |
| category | string | Asset category | "futures" | Yes |
| unit | string | Price unit | "bushel" | Yes |
| description | string | Full description | "Wheat Futures (CBOT)" | Yes |
| raw_price | float | Raw price from API | 590.5 | Yes |
| price_in_dollars | float | Price in dollars | 5.905 | Yes |
| usd_per_ton | float | USD per metric ton | 216.97 | Yes |
| note | string | Processing note | "ok" | Yes |
| extracted_at | string | Extraction timestamp | "2026-04-07T07:51:19.614937+00:00" | Yes |

### 1.3 GrainTrade UA Parser

**Source**: graintrade.com.ua
**Format**: JSON Array
**Update Frequency**: Real-time (offers update frequently)

| Field | Type | Description | Example | Required |
|-------|------|-------------|---------|----------|
| _staging_timestamp | string | ISO timestamp when staged | "2026-04-07T10:29:52.045675" | Yes |
| _source | string | Source identifier | "graintradecomua" | Yes |
| _staging_id | string | Unique staging identifier | "graintradecomua_1775546992.045675" | Yes |
| source_url | string | Source page URL | "https://graintrade.com.ua/birzha?Ad_page=1" | Yes |
| page | integer | Pagination page number | 1 | Yes |
| offer_date | string | Offer publication date/time | "07.04.2026 12:48" | Yes |
| company | string | Company name (Ukrainian) | "ФОП Наварчук Т.В." | Yes |
| offer_type | string | Buy/Sell indicator | "ПРОДАМ" (Sell) | Yes |
| crop | string | Crop type (Ukrainian) | "пшениця 4 клас" | Yes |
| volume_tons | string | Volume in tons | "100" | Yes |
| price_raw | string | Raw price string | "7300 грн" | Yes |
| price_value | float | Parsed price value | 7300.0 | Yes |
| currency | string | Price currency | "грн" | Yes |
| delivery_term | string | Incoterms | "FCA" | Yes |
| basis | string | Delivery location | "Меджибіж, Летичівський р-н..." | Yes |
| location | string | Location code | "0" | No |
| details | string | Additional details | "білок мінім 38%" | No |
| detail_url | string | Offer detail URL | "https://graintrade.com.ua/..." | Yes |
| note | string | Processing note | "ok" | Yes |
| extracted_at | string | Extraction timestamp | "2026-04-07T10:28:38.717833+00:00" | Yes |

### 1.4 Tripoli Land Parser

**Source**: tripoli.land
**Format**: JSON Array
**Update Frequency**: Daily/Weekly

| Field | Type | Description | Example | Required |
|-------|------|-------------|---------|----------|
| _staging_timestamp | string | ISO timestamp when staged | "2026-04-07T09:42:10.292014" | Yes |
| _source | string | Source identifier | "tripoli_land" | Yes |
| _staging_id | string | Unique staging identifier | "tripoli_land_1775544130.292014" | Yes |
| company_slug | string | Company URL slug | "nibulon" | Yes |
| company_name | string | Company name | "Нібулон" | Yes |
| storage_type | string | Storage facility type | "Порт" | Yes |
| storage_location | string | Facility location | "Миколаївський термінал - Нібулон" | Yes |
| address_region | string | Administrative region | "Миколаївська, Миколаївський" | Yes |
| culture_ua | string | Crop type (Ukrainian) | "Пшениця 3 клас" | Yes |
| category_name | string | Category name (English) | "Wheat 3rd grade" | Yes |
| price | string | Price string | "10400" | Yes |
| price_uah_per_ton | float | Price in UAH per ton | 10400.0 | Yes |
| source_url | string | Company page URL | "https://tripoli.land/ua/companies/nibulon" | Yes |
| note | string | Processing note | "ok" | Yes |
| extracted_at | string | Extraction timestamp | "2026-04-07T09:41:47.129662+00:00" | Yes |

## 2. Transformed Data Structures (Silver Layer)

### 2.1 Unified Commodity Price Record

After transformation, all sources are normalized to a common schema:

| Field | Type | Description | Transformation Notes |
|-------|------|-------------|---------------------|
| record_id | string | Unique record identifier | Generated UUID |
| source_key | integer | Foreign key to dim_source | From dimension lookup |
| commodity_key | integer | Foreign key to dim_commodity | From dimension lookup |
| market_key | integer | Foreign key to dim_market | From dimension lookup |
| currency_key | integer | Foreign key to dim_currency | From dimension lookup |
| date_key | integer | Foreign key to dim_date | From date parsing |
| price | float | Normalized price in USD | Converted using FX rates |
| price_uah | float | Original price in UAH | For reference |
| volume | float | Volume in metric tons | Standardized unit |
| price_type | string | Price classification | spot, futures, storage, etc. |
| delivery_term | string | Incoterms code | FCA, CPT, CIF, etc. |
| quality_grade | string | Quality specification | 1st grade, 2nd grade, etc. |
| region | string | Geographic region | Ukraine oblast, etc. |
| company | string | Company name | Standardized |
| offer_type | string | buy/sell indicator | BUY, SELL |
| source_timestamp | datetime | Original source timestamp | Parsed and normalized |
| processed_at | datetime | Transformation timestamp | Current processing time |

### 2.2 Transformation Mappings

#### Commodity Name Standardization
| Source Name (Ukrainian) | Standard Name (English) | Category |
|-------------------------|-------------------------|----------|
| пшениця | Wheat | Grain |
| кукурудза | Corn | Grain |
| соя | Soybeans | Oilseed |
| ячмінь | Barley | Grain |
| жито | Rye | Grain |
| овес | Oats | Grain |
| соняшник | Sunflower | Oilseed |

#### Price Type Classification
| Source | Original Type | Standardized Type | Notes |
|--------|---------------|-------------------|-------|
| YFinance | futures | futures_close | Daily closing price |
| GrainTrade UA | ПРОДАМ/КУПЛЮ | spot_offer | Spot market offer |
| Tripoli Land | storage | storage_rate | Storage/handling fee |
| Currency | official | fx_rate | Exchange rate |

#### Unit Conversions
| Original Unit | Target Unit | Conversion Factor | Notes |
|---------------|-------------|-------------------|-------|
| bushel (Wheat) | metric ton | 0.0272155 | CBOT wheat bushel |
| bushel (Corn) | metric ton | 0.056699 | CBOT corn bushel |
| bushel (Soybeans) | metric ton | 0.0272155 | CBOT soybean bushel |
| UAH | USD | Variable | Uses current FX rate |

## 3. Warehouse Schema (Gold Layer)

### 3.1 Dimension Tables

#### dim_date
| Column | Type | Description | Example |
|--------|------|-------------|---------|
| date_key | INTEGER | Surrogate key (YYYYMMDD) | 20260407 |
| calendar_date | DATE | Actual date | 2026-04-07 |
| year | INTEGER | Year | 2026 |
| quarter | INTEGER | Quarter (1-4) | 2 |
| month | INTEGER | Month (1-12) | 4 |
| day | INTEGER | Day of month | 7 |
| week_of_year | INTEGER | ISO week number | 15 |
| day_of_week | VARCHAR(10) | Day name | Monday |
| is_weekend | BOOLEAN | Weekend flag | false |
| is_holiday | BOOLEAN | Trading holiday | false |
| trading_status | VARCHAR(20) | trading/active/suspended | active |
| created_at | TIMESTAMP | Record creation time | 2026-04-07 12:00:00 |

#### dim_commodity
| Column | Type | Description | Example |
|--------|------|-------------|---------|
| commodity_key | INTEGER | Surrogate key | 1 |
| commodity_name | VARCHAR(100) | Standard name | Wheat |
| commodity_type | VARCHAR(50) | Type category | Grain |
| category | VARCHAR(50) | Sub-category | Winter Wheat |
| unit | VARCHAR(20) | Standard unit | metric_ton |
| grade | VARCHAR(50) | Quality grade | 3rd Grade |
| origin_country | VARCHAR(50) | Country of origin | Ukraine |
| is_active | BOOLEAN | Active flag | true |
| created_at | TIMESTAMP | Creation time | 2026-04-07 12:00:00 |
| ended_at | TIMESTAMP | End time (SCD) | null |

#### dim_market
| Column | Type | Description | Example |
|--------|------|-------------|---------|
| market_key | INTEGER | Surrogate key | 1 |
| market_name | VARCHAR(100) | Market name | CBOT Wheat Futures |
| exchange | VARCHAR(50) | Exchange name | CBOT |
| country | VARCHAR(50) | Country | USA |
| timezone | VARCHAR(30) | Timezone | America/Chicago |
| trading_hours | VARCHAR(100) | Trading hours | 09:30-15:15 CT |
| is_active | BOOLEAN | Active flag | true |
| created_at | TIMESTAMP | Creation time | 2026-04-07 12:00:00 |
| ended_at | TIMESTAMP | End time (SCD) | null |

#### dim_source
| Column | Type | Description | Example |
|--------|------|-------------|---------|
| source_key | INTEGER | Surrogate key | 1 |
| source_name | VARCHAR(100) | Source name | YFinance |
| parser_type | VARCHAR(50) | Parser class | yfinance_parser |
| data_type | VARCHAR(50) | Data category | futures |
| reliability_rating | INTEGER | 1-5 rating | 4 |
| update_frequency | VARCHAR(50) | Update frequency | daily |
| api_endpoint | VARCHAR(500) | API URL | https://finance.yahoo.com |
| is_active | BOOLEAN | Active flag | true |
| created_at | TIMESTAMP | Creation time | 2026-04-07 12:00:00 |
| ended_at | TIMESTAMP | End time (SCD) | null |

#### dim_currency
| Column | Type | Description | Example |
|--------|------|-------------|---------|
| currency_key | INTEGER | Surrogate key | 1 |
| currency_code | VARCHAR(3) | ISO code | USD |
| currency_name | VARCHAR(50) | Full name | US Dollar |
| country | VARCHAR(50) | Primary country | United States |
| created_at | TIMESTAMP | Creation time | 2026-04-07 12:00:00 |

### 3.2 Fact Table

#### commodity_prices_fact
| Column | Type | Description | Example |
|--------|------|-------------|---------|
| price_id | BIGINT | Surrogate key | 1001 |
| date_key | INTEGER | FK to dim_date | 20260407 |
| commodity_key | INTEGER | FK to dim_commodity | 1 |
| market_key | INTEGER | FK to dim_market | 1 |
| source_key | INTEGER | FK to dim_source | 1 |
| currency_key | INTEGER | FK to dim_currency | 1 |
| price | DECIMAL(15,6) | Price in USD | 216.97 |
| price_uah | DECIMAL(15,2) | Price in UAH | 7900.00 |
| volume | DECIMAL(15,3) | Volume in tons | 100.000 |
| price_type | VARCHAR(20) | Price type | spot_offer |
| delivery_term | VARCHAR(10) | Incoterms | FCA |
| quality_grade | VARCHAR(50) | Quality spec | 3rd Grade |
| region | VARCHAR(100) | Geographic region | Vinnytsia Oblast |
| company | VARCHAR(200) | Company name | Nibulon |
| offer_type | VARCHAR(10) | BUY/SELL | SELL |
| source_timestamp | TIMESTAMP | Original timestamp | 2026-04-07 12:48:00 |
| created_at | TIMESTAMP | Record creation | 2026-04-07 13:00:00 |
| updated_at | TIMESTAMP | Last update | 2026-04-07 13:00:00 |

## 4. Analytical Views

### 4.1 Price Trends View (v_price_trends)
Provides daily price changes and trends.

| Column | Type | Description |
|--------|------|-------------|
| date_key | INTEGER | Date key |
| commodity_name | VARCHAR | Commodity name |
| market_name | VARCHAR | Market name |
| close_price | DECIMAL | Closing price |
| volume | DECIMAL | Trading volume |
| prev_price | DECIMAL | Previous day price |
| price_change | DECIMAL | Absolute change |
| price_change_pct | DECIMAL | Percentage change |

### 4.2 Volatility View (v_commodity_volatility)
Shows price volatility by commodity and time period.

| Column | Type | Description |
|--------|------|-------------|
| commodity_name | VARCHAR | Commodity name |
| year | INTEGER | Year |
| month | INTEGER | Month |
| price_volatility | DECIMAL | Standard deviation |
| avg_volume | DECIMAL | Average volume |
| price_range | DECIMAL | High-Low range |

### 4.3 Source Comparison View (v_source_price_comparison)
Compares prices across different sources for the same commodity.

| Column | Type | Description |
|--------|------|-------------|
| calendar_date | DATE | Date |
| commodity_name | VARCHAR | Commodity name |
| source_name | VARCHAR | Data source |
| close_price | DECIMAL | Price |
| price_type | VARCHAR | Price type |

## 5. Data Quality Rules

### 5.1 Completeness Checks
- All fact records must have valid foreign keys
- Price and volume cannot be null or zero
- Source timestamp must be valid
- Required dimensions cannot be missing

### 5.2 Uniqueness Constraints
- No duplicate records for same (date_key, commodity_key, market_key, source_key)
- Dimension natural keys must be unique within active records

### 5.3 Business Rules
- Prices must be positive
- Volume must be non-negative
- Dates must be within reasonable range (not future dates > 1 year)
- Currency codes must be valid ISO 4217
- Commodity names must match controlled vocabulary

### 5.4 Statistical Validations
- Price outliers detected using IQR method
- Volume anomalies flagged
- Sudden price changes validated against historical patterns

## 6. Data Lineage

### 6.1 Source to Bronze Layer
- Raw JSON data stored in MinIO with staging metadata
- Original source timestamps preserved
- No transformations applied

### 6.2 Bronze to Silver Layer
- Data cleaning (deduplication, null handling)
- Normalization (units, currencies, names)
- Enrichment (dimensions, derived fields)
- Schema standardization

### 6.3 Silver to Gold Layer
- Dimensional modeling (star schema)
- Surrogate key assignment
- SCD Type 2 for dimension changes
- Referential integrity enforcement

This data dictionary serves as the authoritative reference for all data structures in the commodity analytics pipeline. All changes to schemas or transformations must be documented here.</content>
<parameter name="filePath">/home/ikost/Projects/commodity-analytic/docs/DATA_DICTIONARY.md