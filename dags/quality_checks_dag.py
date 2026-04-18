"""
Quality Checks DAG - Validate extraction and staging quality before transformation.
Phase 2: Data Pipeline Orchestration (Quality Checks)

Schedule: Triggered after extraction completes
Depends on: extraction_dag
"""

from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys

from airflow.sdk import DAG, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import settings, Environment  # noqa: E402
from staging.staging_handler import StagingHandler  # noqa: E402
from monitoring.alerting import AlertManager  # noqa: E402

SOURCES = ["currency", "yfinance", "graintradecomua", "tripoli_land"]
REQUIRED_FIELDS = {
    "currency": ["provider", "base_currency", "quote_currency", "rate"],
    "yfinance": ["ticker", "extracted_at", "usd_per_ton", "note"],
    "graintradecomua": ["company", "crop", "offer_date", "offer_type", "price_value"],
    "tripoli_land": ["company_slug", "culture_ua", "price", "extracted_at"],
}
UNIQUE_KEYS = {
    "currency": ["provider", "base_currency", "quote_currency", "extracted_at"],
    "yfinance": ["ticker", "extracted_at"],
    "graintradecomua": ["company", "crop", "offer_date", "offer_type"],
    "tripoli_land": ["company_slug", "culture_ua", "extracted_at"],
}
NUMERIC_FIELDS = {
    "currency": ["rate"],
    "yfinance": ["usd_per_ton"],
    "graintradecomua": ["price_value"],
    "tripoli_land": ["price_uah_per_ton", "price_usd_per_ton"],
}


def _storage_type() -> str:
    return "hetzner" if settings.env == Environment.PROD else "minio"


def _parse_timestamp(value):
    if value is None:
        return None

    if isinstance(value, datetime):
        return value

    if isinstance(value, str):
        for fmt in (
            "%Y-%m-%dT%H:%M:%S.%f%z",
            "%Y-%m-%dT%H:%M:%S%z",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d",
        ):
            try:
                return datetime.strptime(value, fmt).replace(tzinfo=timezone.utc)
            except ValueError:
                continue
    return None


with DAG(
    dag_id="quality_checks_dag",
    default_args={
        "owner": "data-engineering",
        "depends_on_past": False,
        "start_date": datetime(2025, 1, 1),
        "email": [settings.alert_email] if settings.alert_email else [],
        "email_on_failure": True,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    description="Run data quality checks on extracted staging data before transformation",
    schedule=None,  # Triggered by extraction_dag completion
    catchup=False,
    tags=["quality", "monitoring", "daily"],
) as dag:

    @task()
    def load_staging_data():
        handler = StagingHandler(storage_type=_storage_type(), layer="bronze")
        data = {}
        for source in SOURCES:
            status = handler.get_staging_status(source)
            records = []
            if status.get("status") == "active":
                records = handler.load_latest_records(source)
            data[source] = {"status": status, "records": records}
        return data

    @task()
    def check_freshness(staging_data):
        now = datetime.now(timezone.utc)
        stale_sources = []
        freshness_notes = {}

        for source, payload in staging_data.items():
            status = payload["status"]
            records = payload["records"]
            if status.get("status") != "active" or not records:
                freshness_notes[source] = "no active staged data"
                stale_sources.append(source)
                continue

            candidate_times = []
            for record in records:
                for field in ("_staging_timestamp", "extracted_at", "offer_date", "timestamp", "date"):
                    ts = _parse_timestamp(record.get(field))
                    if ts:
                        candidate_times.append(ts)

            if not candidate_times:
                freshness_notes[source] = "no timestamp field available"
                continue

            latest = max(candidate_times)
            age = now - latest
            if age > timedelta(hours=24):
                freshness_notes[source] = f"data is stale ({age})"
                stale_sources.append(source)
            else:
                freshness_notes[source] = f"fresh ({age})"

        status = "passed" if not stale_sources else "failed"
        return {
            "status": status,
            "stale_sources": len(stale_sources),
            "details": freshness_notes,
        }

    @task()
    def check_completeness(staging_data):
        missing_counts = {}
        total_missing = 0

        for source, payload in staging_data.items():
            records = payload["records"]
            required = REQUIRED_FIELDS.get(source, [])
            missing = 0
            for record in records:
                for field in required:
                    if record.get(field) in (None, "", []):
                        missing += 1
            missing_counts[source] = missing
            total_missing += missing

        status = "passed" if total_missing == 0 else "failed"
        return {
            "status": status,
            "missing_fields": total_missing,
            "details": missing_counts,
        }

    @task()
    def check_uniqueness(staging_data):
        duplicate_counts = {}
        total_duplicates = 0

        for source, payload in staging_data.items():
            records = payload["records"]
            keys = UNIQUE_KEYS.get(source, [])
            seen = set()
            duplicates = 0
            for record in records:
                key = tuple(record.get(field) for field in keys)
                if key in seen:
                    duplicates += 1
                else:
                    seen.add(key)
            duplicate_counts[source] = duplicates
            total_duplicates += duplicates

        status = "passed" if total_duplicates == 0 else "failed"
        return {
            "status": status,
            "duplicates": total_duplicates,
            "details": duplicate_counts,
        }

    @task()
    def check_data_ranges(staging_data):
        out_of_range = {}
        total_issues = 0

        for source, payload in staging_data.items():
            records = payload["records"]
            fields = NUMERIC_FIELDS.get(source, [])
            issues = 0
            for record in records:
                for field in fields:
                    value = record.get(field)
                    if value is None:
                        continue
                    try:
                        numeric = float(value)
                        if numeric <= 0:
                            issues += 1
                    except (TypeError, ValueError):
                        issues += 1
            out_of_range[source] = issues
            total_issues += issues

        status = "passed" if total_issues == 0 else "failed"
        return {
            "status": status,
            "out_of_range": total_issues,
            "details": out_of_range,
        }

    @task()
    def generate_quality_report(freshness, completeness, uniqueness, ranges):
        summary = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "freshness": freshness,
            "completeness": completeness,
            "uniqueness": uniqueness,
            "ranges": ranges,
        }
        failed_checks = [
            check
            for check in (freshness, completeness, uniqueness, ranges)
            if check.get("status") != "passed"
        ]
        summary["overall_status"] = "FAILED" if failed_checks else "PASSED"
        return summary

    @task()
    def alert_on_failures(quality_report):
        alert_manager = AlertManager(
            slack_webhook_url=settings.slack_webhook,
            email_recipients=settings.alert_recipients,
            smtp_server=settings.smtp_server,
            smtp_port=settings.smtp_port,
            smtp_user=settings.smtp_user,
            smtp_password=settings.smtp_password,
            smtp_use_tls=settings.smtp_use_tls,
        )
        if quality_report.get("overall_status") == "FAILED":
            details = {
                "freshness": quality_report["freshness"],
                "completeness": quality_report["completeness"],
                "uniqueness": quality_report["uniqueness"],
                "ranges": quality_report["ranges"],
            }
            alert_manager.alert_quality_check_failure(
                "daily_quality_checks", str(details)
            )
            return {"alert_sent": True}

        return {"alert_sent": False}

    def choose_next_step(quality_report):
        return "trigger_transformation" if quality_report.get("overall_status") == "PASSED" else "halt_transformation"

    staging_data = load_staging_data()
    freshness = check_freshness(staging_data)
    completeness = check_completeness(staging_data)
    uniqueness = check_uniqueness(staging_data)
    ranges = check_data_ranges(staging_data)

    report = generate_quality_report(freshness, completeness, uniqueness, ranges)
    alert = alert_on_failures(report)

    decide = BranchPythonOperator(
        task_id="decide_transformation_path",
        python_callable=choose_next_step,
        op_args=[report],
    )

    trigger_transformation = TriggerDagRunOperator(
        task_id="trigger_transformation_dag",
        trigger_dag_id="transformation_dag",
        conf={"triggered_by": "quality_checks_dag"},
        wait_for_completion=False,
    )

    halt_transformation = EmptyOperator(task_id="halt_transformation")

    alert >> decide
    decide >> [trigger_transformation, halt_transformation]
