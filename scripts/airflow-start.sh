#!/usr/bin/env bash
# Start / stop / status / restart all Airflow 3.x processes locally.
#
# Usage:
#   bash scripts/airflow-start.sh [start|stop|restart|status]
#   Default action is "start".
#
# Processes managed:
#   api-server        — REST API + UI  (port 8080)
#   scheduler         — DAG scheduling
#   dag-processor     — DAG file parsing (separate in Airflow 3)
#   triggerer         — Deferred task triggers
#   celery-worker     — Task execution via Celery/Redis
#
# Logs:  $AIRFLOW_HOME/logs/local/
# PIDs:  /tmp/airflow-*.pid
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/airflow-env.sh"

# Resolve Airflow executable from PATH or local virtualenv.
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

LOG_DIR="$AIRFLOW_HOME/logs/local"
PID_DIR="/tmp"

mkdir -p "$LOG_DIR"

# ── Process table ──────────────────────────────────────────────────────────────
# name | airflow subcommand(s)
declare -A CMDS=(
    [api-server]="api-server --port ${AIRFLOW_API_SERVER_PORT:-8888}"
    [scheduler]="scheduler"
    [dag-processor]="dag-processor"
    [triggerer]="triggerer"
    [celery-worker]="celery worker"
)

# Ordered startup sequence (dependency order)
STARTUP_ORDER=(api-server scheduler dag-processor triggerer celery-worker)

# ── Helpers ────────────────────────────────────────────────────────────────────
pid_file()  { echo "$PID_DIR/airflow-${1}.pid"; }
log_file()  { echo "$LOG_DIR/airflow-${1}.log"; }

is_running() {
    local pidfile; pidfile="$(pid_file "$1")"
    [[ -f "$pidfile" ]] && kill -0 "$(cat "$pidfile")" 2>/dev/null
}

start_process() {
    local name="$1"
    local cmd="${CMDS[$name]}"

    if is_running "$name"; then
        echo "  [SKIP]    $name already running (PID $(cat "$(pid_file "$name")"))"
        return
    fi

    echo "  [START]   $name → $(log_file "$name")"
    # shellcheck disable=SC2086
    nohup "$AIRFLOW_BIN" $cmd >> "$(log_file "$name")" 2>&1 &
    echo $! > "$(pid_file "$name")"
}

stop_process() {
    local name="$1"
    local pidfile; pidfile="$(pid_file "$name")"

    if ! is_running "$name"; then
        echo "  [SKIP]    $name is not running"
        return
    fi

    local pid; pid=$(cat "$pidfile")
    echo "  [STOP]    $name (PID $pid)"
    kill -SIGTERM "$pid" 2>/dev/null || true

    # Wait up to 15 s for graceful shutdown
    local i=0
    while kill -0 "$pid" 2>/dev/null && (( i < 15 )); do
        sleep 1
        (( i += 1 ))
    done
    kill -0 "$pid" 2>/dev/null && kill -SIGKILL "$pid" || true
    rm -f "$pidfile"
}

status_all() {
    echo "Airflow process status:"
    for name in "${STARTUP_ORDER[@]}"; do
        if is_running "$name"; then
            printf "  %-20s  RUNNING  (PID %s)\n" "$name" "$(cat "$(pid_file "$name")")"
        else
            printf "  %-20s  STOPPED\n" "$name"
        fi
    done
}

report_failed_starts() {
    local had_failure=0
    for name in "${STARTUP_ORDER[@]}"; do
        if ! is_running "$name"; then
            had_failure=1
            echo ""
            echo "  [FAILED]  $name did not stay running"
            if [[ -f "$(log_file "$name")" ]]; then
                echo "  Last log lines from $(log_file "$name"):"
                tail -n 20 "$(log_file "$name")"
            fi
        fi
    done

    return "$had_failure"
}

start_all() {
    echo "Starting Airflow 3 local stack..."
    for name in "${STARTUP_ORDER[@]}"; do
        start_process "$name"
    done
    sleep 2
    echo ""
    status_all
    if ! report_failed_starts; then
        echo ""
        echo "One or more Airflow services failed to start. See logs above."
        return 1
    fi
    echo ""
    echo "Airflow executable:    $AIRFLOW_BIN"
    echo ""
    echo "API / UI available at:  http://localhost:${AIRFLOW_API_SERVER_PORT:-8888}"
    echo "Flower (optional):      $AIRFLOW_BIN celery flower"
    echo ""
    echo "Tail logs with:"
    for name in "${STARTUP_ORDER[@]}"; do
        echo "  tail -f $(log_file "$name")"
    done
}

stop_all() {
    echo "Stopping Airflow 3 local stack..."
    # Reverse order
    for (( i=${#STARTUP_ORDER[@]}-1; i>=0; i-- )); do
        stop_process "${STARTUP_ORDER[$i]}"
    done
    echo "All processes stopped."
}

# ── Main ───────────────────────────────────────────────────────────────────────
ACTION="${1:-start}"

case "$ACTION" in
    start)   start_all ;;
    stop)    stop_all ;;
    restart) stop_all; echo ""; start_all ;;
    status)  status_all ;;
    *)
        echo "Usage: $0 [start|stop|restart|status]"
        exit 1
        ;;
esac
