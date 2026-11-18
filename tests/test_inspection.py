"""The result report must describe persisted data without changing the lake."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from deltalake.exceptions import DeltaError

from src.inspection import export_report, inspect_lake
from src.scenario import run


def test_report_reads_actual_snapshots_and_leaves_lake_untouched(tmp_path: Path) -> None:
    root = tmp_path / "lake<demo>"
    run(root)
    before = {path: path.stat().st_mtime_ns for path in root.rglob("*") if path.is_file()}

    output = tmp_path / "report"
    snapshot = export_report(root, output)

    assert snapshot["tables"]["bronze"]["row_count"] == 15
    assert snapshot["pinned_bronze"] == {"version": 2, "readable": True, "row_count": 14}
    assert snapshot["tables"]["silver"]["row_count"] == 8
    assert [row["order_id"] for row in snapshot["tables"]["silver"]["records"] if row["status"] == "canceled"] == [
        2,
        5,
        7,
    ]
    assert snapshot["checks"] == {
        "silver_matches_corrected_bronze": True,
        "silver_duplicate_order_ids": 0,
        "gold_matches_silver": True,
        "gold_total_orders": 8,
    }
    assert json.loads((output / "tables.json").read_text()) == snapshot
    document = (output / "index.html").read_text()
    assert "VERIFIED" in document
    assert "lake&lt;demo&gt;" in document
    assert "lake<demo>" not in document
    assert "<td>7</td><td>2026-11-01</td>" in document
    assert {path: path.stat().st_mtime_ns for path in root.rglob("*") if path.is_file()} == before


def test_missing_lake_does_not_create_report_directory(tmp_path: Path) -> None:
    output = tmp_path / "report"
    with pytest.raises(DeltaError):
        export_report(tmp_path / "missing", output)
    assert not output.exists()


def test_unavailable_optional_bronze_version_is_reported_without_failing(tmp_path: Path) -> None:
    root = tmp_path / "lake"
    run(root)
    snapshot = inspect_lake(root, bronze_version=999)
    assert snapshot["pinned_bronze"] == {"version": 999, "readable": False, "row_count": None}
    assert snapshot["checks"]["silver_matches_corrected_bronze"] is True
