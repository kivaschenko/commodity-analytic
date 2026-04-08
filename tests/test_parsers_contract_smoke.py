"""Smoke tests for parser contract consistency across active sources."""

from __future__ import annotations

from typing import Any

import pytest

from parser_services.currency_parser import CurrencyParser
from parser_services.graintradecomua_parser import GrainTradeComUaParser
from parser_services.tripoli_land_parser import TripoliLandParser
from parser_services.yfinance_parser import YFinanceParser


def _active_parser_instances() -> list[tuple[str, Any]]:
    return [
        ("yfinance", YFinanceParser(storage_type="minio")),
        ("currency", CurrencyParser(storage_type="minio")),
        ("tripoli_land", TripoliLandParser(storage_type="minio")),
        ("graintradecomua", GrainTradeComUaParser(storage_type="minio", parse_history=False)),
    ]


@pytest.mark.smoke
@pytest.mark.parametrize("source,parser", _active_parser_instances())
def test_parse_and_stage_success_contract(monkeypatch: pytest.MonkeyPatch, source: str, parser: Any) -> None:
    sample_records = [{"note": "ok", "source": source}]

    monkeypatch.setattr(parser, "parse", lambda: sample_records)
    monkeypatch.setattr(parser, "_stage_records", lambda records, storage_type, 
                        file_ext="json": f"s3://bronze-layer/{source}/smoke.json")

    result = parser.parse_and_stage(storage_type="minio")

    assert result["status"] == "success"
    assert result["source"] == source
    assert result["record_count"] == 1
    assert result["storage_type"] == "minio"
    assert result["staged_path"].startswith(f"s3://bronze-layer/{source}/")


@pytest.mark.smoke
@pytest.mark.parametrize("source,parser", _active_parser_instances())
def test_parse_and_stage_no_data_contract(monkeypatch: pytest.MonkeyPatch, source: str, parser: Any) -> None:
    monkeypatch.setattr(parser, "parse", lambda: [])

    result = parser.parse_and_stage(storage_type="minio")

    assert result == {
        "status": "no_data",
        "source": source,
        "record_count": 0,
        "storage_type": "minio",
        "staged_path": None,
    }
