#!/usr/bin/env bash
# One-time (or re-runnable) setup for running Airflow 3.x locally.
# Run as the user who will own the Airflow processes.
#
#   bash scripts/airflow-setup.sh
#
# What it does:
#   1. Sources env vars
#   2. Checks PostgreSQL and Redis connectivity
#   3. Creates the Airflow PostgreSQL database/user if absent
#   4. Generates a Fernet key if none is set
#   5. Runs DB migrations  (airflow db migrate)
#   6. Creates the admin user
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/airflow-env.sh"

# Resolve Airflow executable from active shell or common local virtualenv path.
AIRFLOW_BIN="$(command -v airflow || true)"
if [[ -z "$AIRFLOW_BIN" ]]; then
    for candidate in "$SCRIPT_DIR/../venv/bin/airflow" "$SCRIPT_DIR/../.venv/bin/airflow"; do
        if [[ -x "$candidate" ]]; then
            AIRFLOW_BIN="$candidate"
            break
        fi
    done
fi
if [[ -z "$AIRFLOW_BIN" ]]; then
    echo "ERROR: airflow command not found. Activate your venv first or install Airflow in ./venv." >&2
    echo "Hint: source venv/bin/activate && pip install -r requirements.txt" >&2
    exit 1
fi

PG_USER="${PG_SUPERUSER:-postgres}"     # local superuser to create the airflow DB/role
AIRFLOW_DB_USER=airflow
AIRFLOW_DB_PASS=airflow
AIRFLOW_DB_NAME=airflow

ADMIN_USER="${AIRFLOW_ADMIN_USER:-admin}"
ADMIN_PASS="${AIRFLOW_ADMIN_PASS:-admin}"
ADMIN_EMAIL="${AIRFLOW_ADMIN_EMAIL:-admin@example.com}"

# ── 1. Check PostgreSQL ────────────────────────────────────────────────────────
echo "==> Checking PostgreSQL..."
if ! pg_isready -h localhost -U "$PG_USER" -q; then
    echo "ERROR: PostgreSQL is not running on localhost. Start it first." >&2
    exit 1
fi
echo "    PostgreSQL OK"

# ── 2. Check Redis ─────────────────────────────────────────────────────────────
echo "==> Checking Redis..."
# Parse AIRFLOW__CELERY__BROKER_URL (assumes format: redis://[user:pass@]host:port/db)
BROKER_URL="${AIRFLOW__CELERY__BROKER_URL}"
if [[ "$BROKER_URL" =~ redis://([^:@]*:[^@]*@)?([^:]+):([0-9]+) ]]; then
    REDIS_USERPASS="${BASH_REMATCH[1]}"  # user:pass@ (with @, or empty)
    REDIS_HOST="${BASH_REMATCH[2]}"
    REDIS_PORT="${BASH_REMATCH[3]}"
    
    # Extract password if present (format: user:pass@)
    REDIS_PASS=""
    if [[ -n "$REDIS_USERPASS" ]]; then
        REDIS_PASS="${REDIS_USERPASS%@}"  # Remove trailing @
        REDIS_PASS="${REDIS_PASS#*:}"     # Remove user: prefix, keep only password
    fi
    
    # Build redis-cli command with optional authentication
    REDIS_CMD=(redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT")
    if [[ -n "$REDIS_PASS" ]]; then
        REDIS_CMD+=(-a "$REDIS_PASS")
    fi
    
    if ! "${REDIS_CMD[@]}" ping | grep -q PONG; then
        echo "ERROR: Redis is not accessible at $REDIS_HOST:$REDIS_PORT. Check your broker URL or server." >&2
        exit 1
    fi
    echo "    Redis OK at $REDIS_HOST:$REDIS_PORT"
else
    echo "ERROR: Invalid AIRFLOW__CELERY__BROKER_URL format. Expected redis://[user:pass@]host:port/db" >&2
    exit 1
fi

# ── 3. Create DB user and database (idempotent) ────────────────────────────────
echo "==> Creating PostgreSQL role and database (if absent)..."
sudo -u "$PG_USER" psql -tc \
    "SELECT 1 FROM pg_roles WHERE rolname='$AIRFLOW_DB_USER'" \
    | grep -q 1 || \
    sudo -u "$PG_USER" psql -c \
        "CREATE ROLE $AIRFLOW_DB_USER WITH LOGIN PASSWORD '$AIRFLOW_DB_PASS';"

sudo -u "$PG_USER" psql -tc \
    "SELECT 1 FROM pg_database WHERE datname='$AIRFLOW_DB_NAME'" \
    | grep -q 1 || \
    sudo -u "$PG_USER" psql -c \
        "CREATE DATABASE $AIRFLOW_DB_NAME OWNER $AIRFLOW_DB_USER;"
echo "    Database ready"

# ── 4. Generate Fernet key if not set ──────────────────────────────────────────
if [[ -z "${AIRFLOW__CORE__FERNET_KEY:-}" ]]; then
    echo "==> Generating Fernet key..."
    FERNET_KEY=$(python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())")
    echo ""
    echo "    !! Add this to your shell profile or a secrets file:"
    echo "    export AIRFLOW__CORE__FERNET_KEY='$FERNET_KEY'"
    echo ""
    export AIRFLOW__CORE__FERNET_KEY="$FERNET_KEY"
else
    echo "==> Fernet key already set, skipping generation."
fi

# ── 5. DB migrations ───────────────────────────────────────────────────────────
echo "==> Running airflow db migrate..."
"$AIRFLOW_BIN" db migrate
echo "    Migrations complete"

# ── 6. Create admin user (idempotent) ─────────────────────────────────────────
echo "==> Creating admin user '$ADMIN_USER'..."
"$AIRFLOW_BIN" users create \
    --username "$ADMIN_USER" \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email "$ADMIN_EMAIL" \
    --password "$ADMIN_PASS" 2>/dev/null || echo "    User already exists, skipping."

echo ""
echo "Setup complete. You can now run:  bash scripts/airflow-start.sh"
