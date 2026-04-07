"""Smoke test for Yahoo parser staging into MinIO bronze bucket."""

from __future__ import annotations

import os
from typing import Any

import boto3
import pytest

from parser_services.yfinance_parser import YFinanceParser


def _minio_client() -> Any:
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
        region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
    )


def _ensure_minio_available() -> None:
    try:
        _minio_client().list_buckets()
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"MinIO is not reachable for smoke test: {exc}")


@pytest.mark.smoke
def test_yfinance_parse_and_stage_to_bronze(monkeypatch):
    _ensure_minio_available()

    # Make parser deterministic and independent from external Yahoo network.
    monkeypatch.setattr(YFinanceParser, "_fetch_price", staticmethod(lambda _ticker: 500.0))

    parser = YFinanceParser(storage_type="minio")
    result = parser.parse_and_stage(storage_type="minio")

    assert result["status"] == "success"
    assert result["record_count"] > 0
    assert result["staged_path"]

    bucket = os.getenv("MINIO_BUCKET_BRONZE", "bronze-layer")
    objects = _minio_client().list_objects_v2(Bucket=bucket, Prefix="yfinance/")
    assert objects.get("KeyCount", 0) > 0
