# commodity-analytic

**Production-grade commodity analytics pipeline:** Multi-source data extraction → Medallion architecture (bronze/silver/gold layers) → Star schema warehouse → Apache Airflow orchestration → ML-ready feature engineering. Demonstrates modern data stack best practices with data quality validation, monitoring, and operational resilience.

**Stack:** Apache Airflow 3.0+, Python 3.10+, PostgreSQL/Snowflake, S3/MinIO, PySpark, Pandas, Docker

### Key Features & Skills Demonstrated

**Architecture & Design:**
- ✅ Medallion architecture (bronze → silver → gold layers)
- ✅ Star schema OLAP design with SCD Type 2 slowly changing dimensions
- ✅ Data quality-first approach with multi-stage validation
- ✅ Modular 7-phase pipeline design for independent testing & deployment

**Data Engineering:**
- ✅ Multi-source data extraction (Yahoo Finance, Ukrainian markets)
- ✅ Incremental loading with upsert patterns
- ✅ Data cleaning, normalization, and enrichment
- ✅ Comprehensive data quality checks (duplicates, anomalies, freshness)
- ✅ Feature engineering for ML models (lag, rolling, momentum, seasonal)

**Orchestration & Reliability:**
- ✅ 5 specialized Airflow DAGs with clear dependencies
- ✅ Retry logic, exponential backoff, SLA monitoring
- ✅ Backfill support for historical data
- ✅ Alerting & comprehensive monitoring

**Production Readiness:**
- ✅ Structured JSON logging and operation tracking
- ✅ Health checks and performance monitoring
- ✅ Slack/email alerting with deduplication
- ✅ Test framework and integration test patterns

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

This repository supports running Apache Airflow 3.1.8 locally (without Docker), while using local PostgreSQL and Redis.

Prerequisites:
- Python 3.12+
- PostgreSQL running on localhost:5432
- Redis running on localhost:6379

### 1. Install dependencies

~~~bash
python -m venv venv
source venv/bin/activate

# Recommended: install from pinned project requirements
pip install -r requirements.txt -c my-constraints.txt
~~~

The helper scripts autodetect both `venv` and `.venv`.

### 2. Load Airflow environment variables

Use the helper script:

~~~bash
source scripts/airflow-env.sh
~~~

This sets local runtime paths and Airflow 3 variables, including:
- Celery executor
- Simple auth manager compatible with this Airflow 3.1.8 install
- PostgreSQL metadata DB connection
- Redis broker URL
- Result backend
- Execution API server URL
- Fernet/JWT/API secrets

### 3. One-time setup

Run once per environment (safe to rerun):

~~~bash
bash scripts/airflow-setup.sh
~~~

Setup script actions:
- Verifies PostgreSQL and Redis connectivity
- Creates Airflow DB role/database if missing
- Runs database migrations
- Creates admin user
- Generates Fernet key if not provided

For production, move Fernet/JWT/API secrets out of the repository and inject them from your shell profile, an environment file, or systemd overrides.

### 4. Start/stop local Airflow services

Start all required Airflow 3 processes:

~~~bash
bash scripts/airflow-start.sh
~~~

Other management actions:

~~~bash
bash scripts/airflow-start.sh status
bash scripts/airflow-start.sh restart
bash scripts/airflow-start.sh stop
~~~

Services started by script:
- api-server
- scheduler
- dag-processor
- triggerer
- celery worker

Logs are written to logs/local.

### 5. Run Airflow commands manually (advanced)

If you prefer process-by-process control:

~~~bash
source scripts/airflow-env.sh

# Core checks
airflow info
airflow db check

# Airflow 3 processes
airflow api-server
airflow scheduler
airflow dag-processor
airflow triggerer
airflow celery worker
~~~

### 6. DAG operations

Useful commands for DAG lifecycle and manual runs:

~~~bash
source scripts/airflow-env.sh

# List and inspect
airflow dags list
airflow dags show extraction_pipeline_dag

# Pause / unpause
airflow dags pause extraction_pipeline_dag
airflow dags unpause extraction_pipeline_dag

# Trigger DAG manually
airflow dags trigger extraction_pipeline_dag

# Trigger with custom run id and logical date
airflow dags trigger \
  --run-id manual_$(date +%Y%m%d_%H%M%S) \
  --logical-date "$(date -Iseconds)" \
  extraction_pipeline_dag

# List runs and task states
airflow dags list-runs -d extraction_pipeline_dag
airflow tasks states-for-dag-run extraction_pipeline_dag <dag_run_id>
~~~

### 7. API/UI endpoints

- Airflow UI and API: http://localhost:8080
- Health check: http://localhost:8080/api/v2/version

### 8. Optional: background service with systemd

Ready-to-use systemd assets are available in `scripts/systemd/`.

Install the units:

~~~bash
sudo bash scripts/systemd/install-airflow-systemd.sh
~~~

Manage the full stack:

~~~bash
sudo systemctl start airflow.target
sudo systemctl stop airflow.target
sudo systemctl restart airflow.target
sudo systemctl status airflow.target
~~~

Manage individual services:

~~~bash
sudo systemctl status airflow@api-server
sudo systemctl restart airflow@scheduler
sudo systemctl restart airflow@dag-processor
sudo systemctl restart airflow@triggerer
sudo systemctl restart airflow@celery-worker
~~~

Inspect logs with journalctl:

~~~bash
sudo journalctl -u airflow@api-server -f
sudo journalctl -u airflow@scheduler -f
~~~

This gives auto-restart and auto-start on reboot on dedicated hosts such as Hetzner.

### 9. Local logs

When using the shell scripts instead of systemd, process logs are written to:

~~~bash
logs/local/airflow-api-server.log
logs/local/airflow-scheduler.log
logs/local/airflow-dag-processor.log
logs/local/airflow-triggerer.log
logs/local/airflow-celery-worker.log
~~~

You can tail them directly:

~~~bash
tail -f logs/local/airflow-api-server.log
~~~


### 10. Running MinIO with Docker Run

The docker run command starts MinIO in a new container with everything configured in one line.

Here's the basic command:

```bash
docker run -p 9000:9000 -p 9001:9001 \
  --name minio \
  -v ~/minio/data:/data \
  -e "MINIO_ROOT_USER=admin" \
  -e "MINIO_ROOT_PASSWORD=password123" \
  quay.io/minio/minio server /data --console-address ":9001"
```

 - Port `9000` is the API endpoint where your applications connect to upload and download files. This is where S3-compatible clients send their requests.

 - Port `9001` hosts the web console where you manage buckets, set permissions, and monitor storage. You'll use this to verify MinIO is running correctly.

 - The `-v ~/minio/data:/data` flag maps your local directory to the container's storage location. Everything MinIO stores goes into `~/minio/data` on your host machine. When you stop or remove the container, your data stays safe in this directory.

 - Environment variables set your access credentials. `MINIO_ROOT_USER` is your admin username and `MINIO_ROOT_PASSWORD` is the password. These are the credentials you'll use to log into the web console and configure API access.

 - The server /data argument tells MinIO to run in server mode and use /data as the storage directory. The `--console-address ":9001"` flag specifies which port the web console listens on.


### 11. Running MinIO with Docker Compose

Create a `minio-compose.yml` file:

```bash
services:
  minio:
    image: quay.io/minio/minio
    container_name: minio
    ports:
      - "9000:9000"
      - "9001:9001"
    environment:
      MINIO_ROOT_USER: admin
      MINIO_ROOT_PASSWORD: password123
    volumes:
      - ./minio/data:/data
    command: server /data --console-address ":9001"
```

You can now start MinIO with:
`docker-compose --file minio-compose.yml up -d`

## Testing

```
venv/bin/pytest -q tests/*
```