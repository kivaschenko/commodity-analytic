#!/usr/bin/env bash
# Source this file to export all Airflow environment variables:
#   source scripts/airflow-env.sh
#
# Works both for local dev and Hetzner production — paths resolve relative to project root.

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

log() {
    if [[ "${AIRFLOW_ENV_SILENT:-0}" != "1" ]]; then
        echo "$@"
    fi
}

log "Sourcing Airflow environment variables from $PROJECT_DIR/config/airflow.cfg..."

# ── Core paths ─────────────────────────────────────────────────────────────────
export AIRFLOW_HOME="$PROJECT_DIR"
export AIRFLOW_CONFIG="$PROJECT_DIR/config/airflow.cfg"
export AIRFLOW__CORE__DAGS_FOLDER="$PROJECT_DIR/dags"
export AIRFLOW__LOGGING__BASE_LOG_FOLDER="$PROJECT_DIR/logs"

log "Core Airflow paths set:"
log "  AIRFLOW_HOME: $AIRFLOW_HOME"
log "  AIRFLOW_CONFIG: $AIRFLOW_CONFIG"
log "  AIRFLOW__CORE__DAGS_FOLDER: $AIRFLOW__CORE__DAGS_FOLDER"
log "  AIRFLOW__LOGGING__BASE_LOG_FOLDER: $AIRFLOW__LOGGING__BASE_LOG_FOLDER"

# ── Executor ───────────────────────────────────────────────────────────────────
export AIRFLOW__CORE__EXECUTOR=CeleryExecutor

# Default local auth manager that works without extra providers.
export AIRFLOW__CORE__AUTH_MANAGER="${AIRFLOW__CORE__AUTH_MANAGER:-airflow.api_fastapi.auth.managers.simple.simple_auth_manager.SimpleAuthManager}"

log "Airflow executor set to CeleryExecutor."
log "Auth manager: $AIRFLOW__CORE__AUTH_MANAGER"

# ── Database (local PostgreSQL) ────────────────────────────────────────────────
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@localhost:5432/airflow
export AIRFLOW__CELERY__RESULT_BACKEND=db+postgresql://airflow:airflow@localhost:5432/airflow

log "Database connection set to local PostgreSQL with user 'airflow' and database 'airflow'."

# ── Celery broker (local Redis, DB 2) ─────────────────────────────────────────
export AIRFLOW__CELERY__BROKER_URL="${AIRFLOW__CELERY__BROKER_URL:-redis://default:blackoutdaily@65.108.142.153:6379/2}"
if [[ -z "$AIRFLOW__CELERY__BROKER_URL" ]]; then
    log "  [WARNING] AIRFLOW__CELERY__BROKER_URL is not set! "
else
    log "Celery broker set to local Redis on database 2."
fi
# ── Execution API (Airflow 3 — points to the local api-server) ────────────────
export AIRFLOW__CORE__EXECUTION_API_SERVER_URL=http://localhost:8888/execution/

log "Execution API server URL set to http://localhost:8888/execution/"

# ── Security ───────────────────────────────────────────────────────────────────
# Generate once with:  python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
# Then set AIRFLOW__CORE__FERNET_KEY in your shell profile or .env.local
export AIRFLOW__CORE__FERNET_KEY="${AIRFLOW__CORE__FERNET_KEY:-s8ePtofwyGHFpPaoZiH0ZYC-AdTJH3Xn1JSYeCARGds=}"
export AIRFLOW__API_AUTH__JWT_SECRET="${AIRFLOW__API_AUTH__JWT_SECRET:-change_me_jwt_secret}"
export AIRFLOW__API__SECRET_KEY="${AIRFLOW__API__SECRET_KEY:-change_me_api_secret}"

log "Security settings:"
if [[ -z "$AIRFLOW__CORE__FERNET_KEY" ]]; then
    log "  [WARNING] AIRFLOW__CORE__FERNET_KEY is not set! Generate a Fernet key and set it in your environment for secure encryption."
else
    log "  Fernet key is set."
fi
if [[ -z "$AIRFLOW__API_AUTH__JWT_SECRET" ]]; then
    log " [WARNING] AIRFLOW__API_AUTH__JWT_SECRET is not set! Generate an API Auth JWT Secret and set it in your environment."
else
    log "  JWT secret is set."
fi
if [[ -z "$AIRFLOW__API__SECRET_KEY" ]]; then
    log " [WARNING] AIRFLOW__API__SECRET_KEY is not set! Generate an API Secret Key and set it in your environment."
else
    log "  API secret is set."
fi

# ── Behaviour ──────────────────────────────────────────────────────────────────
export AIRFLOW__CORE__LOAD_EXAMPLES=false
export AIRFLOW__CORE__LOAD_DEFAULT_CONNECTIONS=false
export AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=true
export AIRFLOW__CORE__TEST_CONNECTION=Enabled
export AIRFLOW__SCHEDULER__ENABLE_HEALTH_CHECK=true
export AIRFLOW__DAG_PROCESSOR__REFRESH_INTERVAL=60

log "Other Airflow settings:"
log "  Load example DAGs: $AIRFLOW__CORE__LOAD_EXAMPLES"
log "  Load default connections: $AIRFLOW__CORE__LOAD_DEFAULT_CONNECTIONS"
log "  DAGs paused at creation: $AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION"
log "  Test connection on startup: $AIRFLOW__CORE__TEST_CONNECTION"
log "  Scheduler health check: $AIRFLOW__SCHEDULER__ENABLE_HEALTH_CHECK"
log "  DAG processor refresh interval: $AIRFLOW__DAG_PROCESSOR__REFRESH_INTERVAL seconds"
