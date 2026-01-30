# Dependency Management & Deployment Guide

## Overview

This guide covers dependency management decisions and deployment strategies for the commodity-analytic project.

---

## Poetry vs pip: Recommendation

### ✅ **RECOMMENDATION: Use Poetry + Docker Compose**

**Why Poetry over pip:**
- **Deterministic builds**: `poetry.lock` ensures identical environments across dev/staging/prod
- **Dependency resolution**: Automatic conflict resolution with better error messages
- **Dev dependencies**: Separate dev/test/prod environments without manual management
- **Scripts**: Built-in command runners (`poetry run pytest`, `poetry run black`)
- **Publishing**: Easy package publishing if needed later
- **Virtual env management**: Automatic venv handling

**Why NOT pip-only:**
- Manual lock files (pip-tools) are fragile
- Requires careful venv management across developers
- No native dev dependency separation
- Harder to reproduce exact build state

**Why Docker Compose complements this:**
- Isolates entire environment (OS, Python, dependencies)
- Production-ready: Same image in dev/staging/prod
- Easy onboarding: `docker-compose up` instead of pip install + venv setup
- No "works on my machine" issues
- Compatible with Kubernetes/cloud deployment

---

## Project Dependencies Breakdown

### Core Stack
```
apache-airflow 3.0.6         → Orchestration
pandas 2.1+                  → Data processing
pyspark 3.5+                 → Large-scale transformations
sqlalchemy 2.0+              → ORM & database abstraction
```

### Data Integration
```
requests 2.31+               → HTTP requests (API calls)
beautifulsoup4 4.12+         → Web scraping
yfinance 0.2.32              → Yahoo Finance API
boto3 1.42+                  → AWS S3 integration
```

### Data Quality & Warehousing
```
great-expectations 0.17+     → Data quality validation
psycopg2-binary 2.9+         → PostgreSQL driver
snowflake-sqlalchemy 1.5+    → Snowflake connector
pyarrow 13+                  → Parquet format support
```

### Monitoring & Notifications
```
slack-sdk 3.23+              → Slack alerting
python-dotenv 1.0+           → Environment configuration
pydantic 2.0+                → Data validation
```

### Development Tools
```
pytest 7.4+                  → Testing framework
black 23.11+                 → Code formatting
mypy 1.7+                    → Type checking
flake8 6.1+                  → Linting
isort 5.13+                  → Import sorting
```

---

## Setup Instructions

### Option 1: Poetry (Recommended for development)

```bash
# 1. Install Poetry (one-time)
curl -sSL https://install.python-poetry.org | python3 -

# 2. Create virtual environment
poetry install

# 3. Run commands
poetry run python scripts/init_db.py
poetry run pytest
poetry run black .
```

### Option 2: Docker Compose (Recommended for production)

```bash
# 1. Copy environment file
cp .env.example .env

# 2. Start all services
docker-compose up -d

# 3. Access Airflow
# Browser: http://localhost:8080
# Username: admin / Password: admin

# 4. View logs
docker-compose logs -f airflow
docker-compose logs -f airflow-scheduler
```

### Option 3: pip + venv (Manual, not recommended)

```bash
# Only if Poetry not available
python3.10 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

---

## Python Version Compatibility

**Project targets**: Python 3.10+, **excluding 3.13** (Apache Airflow limitation)

**Python 3.14**: ✅ Fully compatible and future-proof.

### Compatibility Notes:
- Apache Airflow 3.0.6 explicitly excludes Python 3.13 (`python !=3.13`)
- Compatible with: Python 3.10, 3.11, 3.12, **3.14+**
- **Why exclude 3.13?** PySpark and other core dependencies have 3.13 issues
- No code changes needed for Python 3.14
- `pyproject.toml` constraint: `python = ">=3.10,<3.13 || >3.13,<4.0"`

---

## Deployment Strategy

### Development Environment
```
Local workstation:
  - Poetry + venv
  - PostgreSQL locally or via Docker
  - Test with `pytest`
  - Format with `black`, `isort`
```

### Staging Environment
```
Docker Compose (single-machine testing):
  - All services in containers
  - Same code as production
  - Full integration testing
  - Volume mounts for code reload
```

### Production Environment
```
Docker + Kubernetes (recommended scaling):
  - Build production image: `docker build -t commodity-analytics:latest .`
  - Push to registry: `docker push`
  - Deploy with Helm/Kustomize
  
OR

Docker Compose (simple deployment):
  - Standalone server with docker-compose
  - Simple horizontal scaling
  - No Kubernetes complexity
```

---

## Docker Compose Services

```yaml
postgres              # Airflow metadata DB
airflow-webserver    # Airflow UI (port 8080)
airflow-scheduler    # DAG scheduler
minio               # S3-compatible storage (bronze/silver layers)
warehouse-db        # PostgreSQL for OLAP warehouse
```

### Key Ports:
- **8080** - Airflow UI
- **5432** - PostgreSQL (Airflow DB)
- **5433** - PostgreSQL (Warehouse)
- **9000** - MinIO S3
- **9001** - MinIO UI

---

## Workflows

### Daily Development
```bash
# Option A: Poetry (faster iteration)
poetry install --sync
poetry run pytest
poetry run python -m black .
poetry run airflow dags list

# Option B: Docker Compose
docker-compose up -d
docker-compose exec airflow airflow dags list
docker-compose logs -f airflow-scheduler
```

### Before Committing
```bash
# Format code
poetry run black .
poetry run isort .

# Type check
poetry run mypy --ignore-missing-imports . --no-error-summary

# Lint
poetry run flake8 . --max-line-length=100

# Run tests
poetry run pytest tests/ -v --cov=.
```

### Build & Deploy to Production
```bash
# Build Docker image
docker build -t my-registry/commodity-analytics:latest .

# Push to registry
docker push my-registry/commodity-analytics:latest

# Deploy
docker pull my-registry/commodity-analytics:latest
docker-compose up -d
```

---

## Troubleshooting

### Poetry Lock File
```bash
# If dependencies conflict
poetry update
poetry lock --no-update

# Clear cache
poetry cache clear . --all
```

### Docker Issues
```bash
# Rebuild images (fresh install)
docker-compose build --no-cache

# Remove all data
docker-compose down -v

# Check container health
docker-compose ps
```

### Python Version
```bash
# Check current Python
poetry env info
python --version

# Switch Python version in poetry
poetry env use python3.14
poetry install --sync
```

---

## Maintenance

### Update Dependencies (Quarterly)
```bash
# Check outdated packages
poetry show --outdated

# Update specific package
poetry update apache-airflow

# Update all
poetry update
poetry lock
```

### Clean Up
```bash
# Remove unused dependencies
poetry remove <package>

# Clean build artifacts
rm -rf build/ dist/ *.egg-info/

# Docker cleanup
docker-compose down
docker system prune -a
```

---

## Cost Considerations for Production

**Docker Compose approach** (cheapest):
- Single t3.medium EC2 (~$30/month)
- RDS PostgreSQL (~$20/month)
- S3 storage (~$1/month per GB)

**Kubernetes approach** (scales):
- EKS cluster (~$70/month) + nodes ($50-200/month)
- RDS (~$20/month)
- S3 (~$1/month per GB)

For your project scale, **Docker Compose on single EC2 is recommended**.

---

## Next Steps

1. **Install Poetry**: `curl -sSL https://install.python-poetry.org | python3 -`
2. **Set up environment**: `poetry install`
3. **Or use Docker**: `docker-compose up -d`
4. **Initialize database**: `poetry run python scripts/init_db.py` or `docker-compose exec airflow python scripts/init_db.py`
5. **Run tests**: `poetry run pytest` or `docker-compose exec airflow pytest`
6. **Start Airflow**: Already running in docker-compose, or `poetry run airflow webserver`

---

## References

- [Poetry Documentation](https://python-poetry.org/docs/)
- [Docker Compose Documentation](https://docs.docker.com/compose/)
- [Apache Airflow Installation](https://airflow.apache.org/docs/apache-airflow/stable/installation/)
- [Best Practices for Python Packaging](https://packaging.python.org/)
