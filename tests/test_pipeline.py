"""Unit tests for the pure pipeline functions, with no storage."""

from __future__ import annotations

import pandas as pd

from src.pipeline import build_gold, build_silver, diff_rows
from src.rules import CorrectedStatusRule, FaultyStatusRule

CHANGES = pd.DataFrame(
    [
        {"order_id": 1, "seq": 1, "order_date": "2026-11-03", "raw_status": "PLACED"},
        {"order_id": 1, "seq": 2, "order_date": "2026-11-03", "raw_status": "CANCELLED"},
        {"order_id": 2, "seq": 1, "order_date": "2026-11-03", "raw_status": "SHIPPED"},
    ]
)


def test_rules_differ_only_for_canceled_orders() -> None:
    gap = diff_rows(build_silver(CHANGES, FaultyStatusRule()), build_silver(CHANGES, CorrectedStatusRule()))
    assert gap.different == (1,)


def test_build_silver_keeps_the_latest_change_by_source_sequence() -> None:
    silver = build_silver(CHANGES, CorrectedStatusRule())
    assert silver.set_index("order_id")["status"].to_dict() == {1: "canceled", 2: "shipped"}


def test_status_rule_only_needs_the_status_operation() -> None:
    class LowercaseRule:
        def status(self, raw_status: str) -> str:
            return raw_status.lower()

    silver = build_silver(CHANGES, LowercaseRule())
    assert silver.set_index("order_id")["status"].to_dict() == {1: "cancelled", 2: "shipped"}


def test_build_gold_counts_orders_by_date_and_status() -> None:
    gold = build_gold(build_silver(CHANGES, CorrectedStatusRule()))
    assert gold.to_dict("records") == [
        {"order_date": "2026-11-03", "status": "canceled", "orders": 1},
        {"order_date": "2026-11-03", "status": "shipped", "orders": 1},
    ]


def test_diff_rows_reports_both_directions() -> None:
    left = pd.DataFrame([{"order_id": 1, "status": "a"}, {"order_id": 2, "status": "b"}])
    right = pd.DataFrame([{"order_id": 2, "status": "b"}, {"order_id": 3, "status": "c"}])
    gap = diff_rows(left, right)
    assert (gap.only_left, gap.only_right, gap.different, gap.same) == ((1,), (3,), (), False)
