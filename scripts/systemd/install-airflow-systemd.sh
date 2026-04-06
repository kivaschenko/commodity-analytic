#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
RUN_USER="${SUDO_USER:-$USER}"
RUN_GROUP="$(id -gn "$RUN_USER")"
SYSTEMD_DIR="/etc/systemd/system"

if [[ "$EUID" -ne 0 ]]; then
    echo "Run with sudo: sudo bash scripts/systemd/install-airflow-systemd.sh" >&2
    exit 1
fi

install_unit() {
    local source_file="$1"
    local target_file="$2"

    sed \
        -e "s|__PROJECT_DIR__|$PROJECT_DIR|g" \
        -e "s|__RUN_USER__|$RUN_USER|g" \
        -e "s|__RUN_GROUP__|$RUN_GROUP|g" \
        "$source_file" > "$target_file"
}

install_unit "$SCRIPT_DIR/airflow@.service" "$SYSTEMD_DIR/airflow@.service"
install_unit "$SCRIPT_DIR/airflow.target" "$SYSTEMD_DIR/airflow.target"

systemctl daemon-reload
systemctl enable airflow.target

echo "Installed systemd units:"
echo "  $SYSTEMD_DIR/airflow@.service"
echo "  $SYSTEMD_DIR/airflow.target"
echo ""
echo "Start the Airflow stack with:"
echo "  sudo systemctl start airflow.target"
echo ""
echo "Check service state with:"
echo "  sudo systemctl status airflow.target"
echo "  sudo systemctl status airflow@api-server"
echo ""
echo "Tail logs with:"
echo "  sudo journalctl -u airflow@api-server -f"
