# Commodity Analytics Pipeline

A locally run data pipeline for commodity prices. Apache Airflow coordinates extraction, validation, transformation, and warehouse loading. The project uses host-installed services; no containers are required.

## Stack

Minimum versions are taken from [`pyproject.toml`](pyproject.toml):

| Component | Minimum version |
|---|---:|
| Python | 3.14 |
| Apache Airflow | 3.3.2 |
| Apache Airflow Celery provider | 3.24.0 |
| Apache Airflow Postgres provider | 7.0.2 |
| PySpark | 4.2.0 |
| pandas | 3.0.6 |
| PyArrow | 25.0.1 |
| SQLAlchemy | 2.1.1 |
| boto3 | 1.43.103 |
| yfinance | 1.7.0 |

`uv.lock` pins the resolved environment; `uv sync --locked` installs from that lock file.

## Pipeline

The Airflow DAGs coordinate the following stages:

1. Extract commodity prices and FX rates from external sources.
2. Store raw data in the MinIO bronze layer and validate it.
3. Clean and enrich data into the silver layer.
4. Load analytical tables into the PostgreSQL warehouse.

The DAGs are in [`dags/`](dags/). Parsers, transformations, and warehouse code are in `parser_services/`, `transformation/`, and `warehouse/`.

## Local setup

Install and start these services directly on your machine:

- PostgreSQL on `localhost:5432`
- Valkey (Redis-compatible) on `localhost:6379`
- MinIO on `localhost:9000`

Install each using your operating system's package manager or the service's official installation instructions. PostgreSQL and Valkey are required to run Airflow with the configured Celery executor. MinIO is used for object storage by the pipeline.

Install [uv](https://docs.astral.sh/uv/getting-started/installation/) and run these commands from the project root:

```bash
# Install Python and project dependencies.
uv python install 3.14
uv sync --locked

# Create a local environment file and set a generated Fernet key and local credentials.
cp .env.example .env

# Generate a Fernet key, then put its output in AIRFLOW__CORE__FERNET_KEY in .env.
uv run python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

# Export .env values and load the project's Airflow configuration.
set -a
source .env
set +a
source scripts/airflow-env.sh

# For new project and empty database create the tables and views
uv run python -m scripts.init_db

# Create the Airflow metadata database and admin user.
uv run bash scripts/airflow-setup.sh

# Start Airflow's API server, scheduler, DAG processor, triggerer, and Celery worker.
uv run bash scripts/airflow-start.sh
```

The setup script creates the local Airflow PostgreSQL role and database; it may ask for `sudo` access. Set `AIRFLOW_ADMIN_USER`, `AIRFLOW_ADMIN_PASS`, and `AIRFLOW_ADMIN_EMAIL` in `.env` to choose the UI account. The example credentials are for local development only.

The API server and UI use `AIRFLOW_API_SERVER_PORT` (8080 in `.env.example`). Open <http://localhost:8080>.

## Operating Airflow

Load the same environment in each new terminal before running Airflow commands:

```bash
set -a
source .env
set +a
source scripts/airflow-env.sh

uv run airflow dags list
uv run airflow dags trigger extraction_dag
uv run bash scripts/airflow-start.sh status
uv run bash scripts/airflow-start.sh stop
```

Airflow logs are written under `logs/local/`.

## Configuration

`.env.example` contains the local Airflow settings and application settings used by `config/settings.py`. Update the database URL, MinIO credentials, and secrets for your machine. Keep `.env` private; do not commit it.

## License

[MIT](LICENSE)
