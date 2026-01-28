# commodity-analytic
### Batch Analytics Platform (Airflow)
**Stack:** Airflow, Python, Postgres, S3/MinIO, Docker, pyspark, Snowflake

**Description:**
Commodity price data pipeline that extracts commodity prices from multiple sources (Yahoo Finance, Investing.com, Ukrainian trading sites), cleanses and transforms data, and materializes it into a dimensional/OLAP warehouse for predictive analytics.

**What to emphasize in CV:**
- DAG design & orchestration
- Data modeling (star schema/OLAP)
- ETL/ELT pipeline development
- Multi-source data integration
- Reliability patterns (retries, SLAs, backfill)
- Data quality & validation

---

## Project Roadmap

### Phase 1: Data Extraction Layer (Current)
**Objective:** Build robust parsers for all commodity price sources

- [x] **Base Parser Framework**
  - Abstract `BaseParser` class for consistent interface
  - Support for multiple storage types (local, S3, cloud)
  - Multiple output formats (JSON, CSV, Parquet)

- [ ] **Data Source Parsers**
  - [x] Yahoo Finance parser (yfinance_parser.py)
  - [x] Investing.com parser (investingcom_parser.py)
  - [x] Ukrainian Trading Site parsers:
    - [x] GrainTrade.com.ua (graintradecomua_parser.py)
    - [x] APK Inform (apk_inform_parser.py)
    - [x] Tripoli Land (tripoli_land_parser.py)
  - [x] Currency parser (currency_parser.py)

- [ ] **Parser Enhancements**
  - Add error handling & retry logic
  - Implement logging & monitoring
  - Handle pagination for large datasets
  - Support incremental/delta extracts

---

### Phase 2: Data Staging & Validation (Next)
**Objective:** Create staging layer with data quality checks

- [ ] **Staging Layer**
  - Raw data staging in S3/MinIO (bronze layer)
  - Data schema validation
  - Duplicate detection
  - Missing value handling
  - Timestamp standardization

- [ ] **Data Quality Framework**
  - Row count validation
  - Schema validation tests
  - Range checks (price, volume bounds)
  - Uniqueness constraints
  - Anomaly detection (spike alerts)

- [ ] **Monitoring & Alerting**
  - Data freshness checks
  - Null/missing data alerts
  - Source availability monitoring
  - Email/Slack notifications on failures

---

### Phase 3: Data Transformation & Cleaning (Planning)
**Objective:** Prepare clean, standardized data for analytics

- [ ] **Transformation Pipeline**
  - Standardize commodity names & units
  - Currency conversion to base currency
  - Data normalization (e.g., prices to USD/ton)
  - Time series handling (fill gaps, interpolation)
  - Outlier detection & treatment

- [ ] **Data Enrichment**
  - Add business date/time dimensions
  - Market context flags (holidays, trading halts)
  - Commodity attributes (type, origin, grades)
  - Historical comparisons (YoY, MoM changes)

- [ ] **Silver Layer**
  - Store cleaned data in Parquet format
  - Implement SCD Type 2 for slowly changing dimensions
  - Create temporary staging tables in Postgres/Snowflake

---

### Phase 4: Dimensional Modeling & OLAP Warehouse (Design)
**Objective:** Build star schema for analytics & prediction

- [ ] **Dimensional Model Design**
  - **Fact Table:** commodity_prices_fact
    - Columns: date_key, commodity_key, market_key, source_key, price, volume, currency
  - **Dimension Tables:**
    - dim_commodity (commodity name, type, unit, category)
    - dim_market (market name, country, exchange, timezone)
    - dim_source (data source, parser type, reliability rating)
    - dim_date (date, quarter, year, fiscal period, trading flags)
    - dim_currency (currency code, exchange rates by date)

- [ ] **Aggregate Tables (for performance)**
  - daily_price_summary (daily OHLC, volume)
  - weekly_commodity_agg
  - monthly_commodity_agg

- [ ] **Snowflake/Postgres Implementation**
  - Create schema in warehouse
  - Implement incremental loading (upsert/merge)
  - Create indexes for fast queries
  - Enable time-travel/versioning (Snowflake feature)

---

### Phase 5: Data Pipeline Orchestration (Integration)
**Objective:** Automate daily/scheduled extraction → transformation → loading

- [ ] **Airflow DAG Development**
  - Daily extraction DAG (schedule: 0 0 * * *)
  - Transformation DAG (runs after extraction)
  - Warehouse load DAG (dimension & fact tables)
  - Data quality checks DAG
  - Backfill & recovery DAGs

- [ ] **Operational Features**
  - Retry logic (exponential backoff)
  - SLA monitoring
  - Dynamic task allocation
  - Catchup & backfill support
  - Slack/email notifications

- [ ] **Infrastructure**
  - Docker containerization
  - Environment configuration (dev, staging, prod)
  - Secret management (API keys, credentials)
  - Resource allocation (CPU, memory limits)

---

### Phase 6: Analytics & ML Preparation (Future)
**Objective:** Expose data for prediction models

- [ ] **Analytics Layer**
  - Create analytical views (price trends, volatility)
  - Aggregate tables for common queries
  - Materialized views for dashboards
  - Historical snapshots for backtesting

- [ ] **Feature Engineering for ML**
  - Time-series features (lag, rolling avg, momentum)
  - Market volatility indices
  - Seasonal decomposition
  - Correlation matrices between commodities
  - Export training datasets in ML-ready format

- [ ] **Prediction Model Support**
  - Time-series forecasting features (ARIMA, Prophet, LSTM)
  - Classification features (price direction, anomaly flags)
  - Feature store implementation
  - Model training data pipeline

---

### Phase 7: Monitoring, Maintenance & Optimization (Ongoing)
**Objective:** Ensure production reliability & performance

- [ ] **Performance Monitoring**
  - Query performance tracking
  - Pipeline execution time SLAs
  - Storage growth monitoring
  - Cost optimization (Snowflake compute)

- [ ] **Maintenance**
  - Incremental data refresh strategy
  - Archive old data (partitioning by date)
  - Backup & disaster recovery procedures
  - Regular data integrity audits

- [ ] **Documentation**
  - Data dictionary & lineage
  - DAG documentation & runbooks
  - Troubleshooting guides
  - Data governance policies

---

## Technology Stack Details

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Orchestration** | Apache Airflow 3.0+ | DAG scheduling, retry, monitoring |
| **Extraction** | Python, requests, yfinance, beautifulsoup | Web scraping & API calls |
| **Staging** | S3/MinIO | Raw data lake (bronze layer) |
| **Transformation** | PySpark, Pandas | Large-scale data processing |
| **Warehouse** | Snowflake (prod) / PostgreSQL (dev) | OLAP database |
| **Storage** | Parquet, CSV | Optimized columnar format |
| **Monitoring** | Airflow UI, Logs, Slack | Operational visibility |
| **IaC** | Docker, docker-compose | Infrastructure as code |

---

## Getting Started

See Development mode section below for setup instructions.


## Development mode

Installation of dependencies.

```
# Using python3.14
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Set environ variables to configure Airflow:
```
export AIRFLOW__CORE__EXECUTOR=LocalExecutor  && \
export AIRFLOW__CORE__LOAD_DEFAULT_CONNECTIONS=False  && \
export AIRFLOW__CORE__LOAD_EXAMPLES=False  && \
export AIRFLOW__CORE__SQL_ALCHEMY_CONN=postgresql://airflow:airflow@postgres:5432/airflow  && \
export AIRFLOW__CORE__DAGS_FOLDER=/home/ikost/Projects/commodity-analytic/dags
```

Run Airflow:
```
# Check database connection
airflow db check

# Perform 
```