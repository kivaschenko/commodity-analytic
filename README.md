# Commodity Analytics Pipeline

> **End-to-end data engineering portfolio project** — production-grade commodity price pipeline built on Apache Airflow 3, demonstrating the full modern data stack from multi-source extraction to ML-ready feature store.

---

## Tech Stack

![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-3.1-017CEE?style=flat-square&logo=apacheairflow&logoColor=white)
![Python](https://img.shields.io/badge/Python-3.10+-3776AB?style=flat-square&logo=python&logoColor=white)
![Apache Spark](https://img.shields.io/badge/PySpark-4.1-E25A1C?style=flat-square&logo=apachespark&logoColor=white)
![Pandas](https://img.shields.io/badge/Pandas-3.0-150458?style=flat-square&logo=pandas&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-4169E1?style=flat-square&logo=postgresql&logoColor=white)
![Snowflake](https://img.shields.io/badge/Snowflake-SQLAlchemy-29B5E8?style=flat-square&logo=snowflake&logoColor=white)
![MinIO](https://img.shields.io/badge/MinIO-S3--compatible-C72E49?style=flat-square&logo=minio&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=flat-square&logo=docker&logoColor=white)
![Great Expectations](https://img.shields.io/badge/Great%20Expectations-1.15-FF6B35?style=flat-square)
![Pydantic](https://img.shields.io/badge/Pydantic-2.x-E92063?style=flat-square&logo=pydantic&logoColor=white)
![pytest](https://img.shields.io/badge/pytest-9.0-0A9EDC?style=flat-square&logo=pytest&logoColor=white)
![Slack](https://img.shields.io/badge/Slack-Alerting-4A154B?style=flat-square&logo=slack&logoColor=white)

---

## What This Project Demonstrates

This project showcases **production-level data engineering skills** across the full pipeline lifecycle:

| Skill Area | Highlights |
|---|---|
| **Orchestration** | 5 specialized Apache Airflow 3 DAGs, DAG chaining via `TriggerDagRunOperator`, retry + SLA |
| **Architecture** | Medallion (bronze → silver → gold), star schema with SCD Type 2 |
| **Data Quality** | Great Expectations validators, anomaly detection, freshness & schema checks |
| **Extraction** | Custom parsers for Yahoo Finance, Ukrainian grain markets, live FX rates |
| **Transformation** | Cleaning, normalization, currency conversion, outlier handling |
| **Warehouse** | Incremental upsert loading, fact + dimension tables, Postgres & Snowflake |
| **Feature Eng.** | Lag, rolling window, momentum, and seasonal features for ML readiness |
| **Monitoring** | Structured JSON logging, health checks, Slack/email alerting with deduplication |
| **Infrastructure** | Docker Compose, environment-based config, secrets management |

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                                     │
│   Yahoo Finance (yfinance)  │  GrainTrade.com.ua   │  Tripoli Land      │
│                             │                      │  Currency/FX API   │
└────────────────┬────────────┴──────────┬───────────┴─────────┬─────────┘
                 │                       │                     │
                 ▼                       ▼                     ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  [Phase 1]  extraction_dag.py   —   Daily @ midnight UTC (0 0 * * *)    │
│  Parser services:  base_parser → yfinance / graintrade / tripoli / fx   │
└──────────────────────────────────┬──────────────────────────────────────┘
                                   │
                                   ▼
                    ┌──────────────────────────┐
                    │   BRONZE LAYER  (MinIO)  │
                    │   Raw JSON / Parquet     │
                    └──────────────┬───────────┘
                                   │
┌──────────────────────────────────▼──────────────────────────────────────┐
│  [Phase 2]  quality_checks_dag.py  —  Great Expectations validation     │
│  Schema checks │ Range bounds │ Duplicate detection │ Anomaly alerts    │
└──────────────────────────────────┬──────────────────────────────────────┘
                                   │
┌──────────────────────────────────▼──────────────────────────────────────┐
│  [Phase 3]  transformation_dag.py  —  Clean → Normalize → Enrich        │
│  Outlier removal │ Currency conversion │ SCD Type 2 dims │ Parquet out  │
└──────────────────────────────────┬──────────────────────────────────────┘
                                   │
                    ┌──────────────▼───────────┐
                    │   SILVER LAYER  (MinIO)  │
                    │   Clean Parquet data     │
                    └──────────────┬───────────┘
                                   │
┌──────────────────────────────────▼──────────────────────────────────────┐
│  [Phase 4]  warehouse_load_dag.py  —  Star schema incremental load      │
│  Fact: commodity_prices_fact  │  Dims: commodity, market, date, source  │
└──────────────────────────────────┬──────────────────────────────────────┘
                                   │
                    ┌──────────────▼───────────┐
                    │   GOLD LAYER  (Postgres  │
                    │   / Snowflake)           │
                    │   Analytics-ready        │
                    └──────────────┬───────────┘
                                   │
┌──────────────────────────────────▼──────────────────────────────────────┐
│  [Phase 5]  Analytics: views, aggregates, ML feature engineering        │
│  Lag / rolling / momentum features  │  Time-series forecasting prep     │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Airflow DAG Overview

| DAG | Schedule | Purpose |
|---|---|---|
| `extraction_dag` | `0 0 * * *` | Pull raw prices from all sources → MinIO bronze layer |
| `quality_checks_dag` | Triggered | Validate schema, ranges, freshness, anomalies |
| `transformation_dag` | Triggered | Clean, normalize, enrich → silver layer |
| `warehouse_load_dag` | Triggered | Upsert fact + dimension tables in Postgres/Snowflake |
| `backfill_dag` | Manual | Historical data recovery for any date range |

All DAGs use chained triggering (`TriggerDagRunOperator`) so a failure in quality checks halts downstream loading automatically.

---

## Screenshots

### Airflow DAG Graph View

> *(screenshot from production server — to be added)*

A live view of the `extraction_dag` graph showing parallel task execution across four data source parsers, with the downstream trigger to `quality_checks_dag`.

---

### Airflow DAGs Dashboard

> *(screenshot from production server — to be added)*

The Airflow UI listing all five DAGs with run history, success/failure indicators, and schedule status.

---

### MinIO Object Storage — Bronze Layer

> *(screenshot from production server — to be added)*

Raw JSON and Parquet files organized in date-partitioned prefixes inside the MinIO `bronze` bucket, as deposited by the extraction DAG.

---

### Data Quality Report (Great Expectations)

> *(screenshot from production server — to be added)*

Great Expectations validation run output showing schema checks, null rate, range bounds, and anomaly detection results per data source.

---

### Warehouse Star Schema — Postgres

> *(screenshot from production server — to be added)*

pgAdmin / psql view of the `commodity_prices_fact` table joined with `dim_commodity`, `dim_market`, and `dim_date`, illustrating the OLAP star schema design.

---

## Project Structure

```
commodity-analytic/
├── dags/
│   ├── extraction_dag.py        # Phase 1: Multi-source extraction
│   ├── quality_checks_dag.py    # Phase 2: Data quality validation
│   ├── transformation_dag.py    # Phase 3: Clean → normalize → enrich
│   ├── warehouse_load_dag.py    # Phase 4: Star schema load
│   └── backfill_dag.py          # Manual historical recovery
│
├── parser_services/             # Pluggable data source extractors
│   ├── base_parser.py           # Abstract base (storage-agnostic)
│   ├── yfinance_parser.py       # Yahoo Finance
│   ├── graintradecomua_parser.py# Ukrainian grain market
│   ├── tripoli_land_parser.py   # Ukrainian land prices
│   └── currency_parser.py       # FX rates
│
├── staging/                     # Bronze layer & validation
│   ├── staging_handler.py
│   ├── validators.py
│   └── data_quality.py
│
├── transformation/              # Silver layer pipeline
│   ├── cleaner.py
│   ├── normalizer.py
│   └── enricher.py
│
├── warehouse/                   # Gold layer — star schema
│   ├── schema.sql
│   ├── models.py
│   └── loader.py
│
├── analytics/                   # Analytical views & ML features
│   ├── views.py
│   ├── aggregates.py
│   └── features.py
│
├── monitoring/                  # Observability
│   ├── health_checks.py
│   ├── alerting.py              # Slack + email
│   └── logging.py               # Structured JSON logs
│
├── config/settings.py           # Dev / staging / prod config
├── tests/                       # pytest suite
├── compose.yml                  # Docker Compose (Airflow + Postgres)
├── minio-compose.yml            # MinIO object storage
└── requirements.txt
```

---

## Quick Start

```bash
# 1. Clone the repo
git clone https://github.com/your-username/commodity-analytic.git
cd commodity-analytic

# 2. Copy and configure environment
cp .env.example .env
# Edit .env with your DB credentials, MinIO keys, Slack webhook, etc.

# 3. Start infrastructure (Airflow + Postgres + MinIO)
docker compose -f compose.yml -f minio-compose.yml up -d

# 4. Initialize the Airflow DB and create admin user
bash scripts/airflow-setup.sh

# 5. Open Airflow UI
open http://localhost:8080
# Default credentials: admin / admin

# 6. Trigger the pipeline manually
airflow dags trigger extraction_dag
```

> See [QUICK_START.md](QUICK_START.md) for detailed environment setup and troubleshooting.

---

## Roadmap

### ✅ Phase 1 — Extraction Layer
- Abstract `BaseParser` with pluggable storage (local / S3 / MinIO)
- Yahoo Finance, GrainTrade.com.ua, Tripoli Land, and FX rate parsers
- Airflow `extraction_dag` with parallel tasks and retry logic

### ✅ Phase 2 — Staging & Data Quality
- Bronze layer storage in MinIO (JSON + Parquet)
- Great Expectations schema, range, uniqueness, and freshness validators
- Anomaly detection with Slack/email alerting

### ✅ Phase 3 — Transformation
- Deduplication, null handling, and outlier removal
- Currency conversion to base currency (USD)
- SCD Type 2 implementation for slowly changing dimensions
- Enrichment: business date attributes, market context flags

### ✅ Phase 4 — Warehouse
- Star schema: `commodity_prices_fact` + 5 dimension tables
- Incremental upsert/merge loading
- Postgres (dev) and Snowflake (prod) support

### ✅ Phase 5 — Orchestration
- 5 chained Airflow 3 DAGs with `TriggerDagRunOperator`
- SLA monitoring, exponential backoff, catchup/backfill
- Docker Compose deployment

### 🔄 Phase 6 — Analytics & ML Preparation *(in progress)*
- [ ] Analytical SQL views (price trends, volatility, YoY/MoM)
- [ ] ML feature engineering: lag, rolling window, momentum, seasonal decomposition
- [ ] Feature store export (Parquet, ML-ready format)
- [ ] Time-series forecasting support (ARIMA, Prophet, LSTM prep)

### 🔲 Phase 7 — Monitoring & Optimization *(planned)*
- [ ] Query performance tracking and storage growth monitoring
- [ ] Partition-based archiving strategy
- [ ] Full data dictionary and lineage documentation
- [ ] Cost optimization (Snowflake compute credits)

---

## License

[MIT](LICENSE)

---

## Local Development Setup

This repository supports running Apache Airflow 3.1 locally (without Docker), alongside a local PostgreSQL and Redis.

**Prerequisites:** Python 3.12+, PostgreSQL on `localhost:5432`, Redis on `localhost:6379`

### 1. Install dependencies

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt -c my-constraints.txt
```

### 2. Load environment variables

```bash
source scripts/airflow-env.sh
```

Sets Celery executor, Postgres metadata DB, Redis broker, Fernet/JWT secrets, and the Airflow 3 execution API URL.

### 3. One-time setup

```bash
bash scripts/airflow-setup.sh
```

Creates the DB role/database, runs Airflow migrations, and creates the admin user.

### 4. Start all services

```bash
bash scripts/airflow-start.sh          # start api-server, scheduler, dag-processor, triggerer, celery worker
bash scripts/airflow-start.sh status   # check running processes
bash scripts/airflow-start.sh stop     # stop all
```

Logs are written to `logs/local/`.

### 5. DAG operations

```bash
source scripts/airflow-env.sh

airflow dags list
airflow dags trigger extraction_dag
airflow dags list-runs -d extraction_dag
airflow tasks states-for-dag-run extraction_dag <run_id>
```

**Airflow UI:** http://localhost:8080 · **Health:** http://localhost:8080/api/v2/version

### 6. Optional: systemd service

```bash
sudo bash scripts/systemd/install-airflow-systemd.sh
sudo systemctl start airflow.target
```

---

*Built to demonstrate production-grade data engineering — feedback and forks welcome.*
