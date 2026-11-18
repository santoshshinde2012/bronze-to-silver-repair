"""Change-feed semantics against real local Delta tables."""

from pathlib import Path

import pandas as pd

from src.models import FeedSummary
from src.repository import DeltaLake


def test_feed_detects_date_changes_and_both_affected_gold_dates(tmp_path: Path) -> None:
    lake = DeltaLake(tmp_path)
    original = pd.DataFrame(
        [
            {"order_id": 1, "order_date": "2026-11-03", "status": "shipped"},
            {"order_id": 2, "order_date": "2026-11-03", "status": "shipped"},
        ]
    )
    lake.overwrite_silver(original)
    replacement = original.copy()
    replacement.loc[replacement["order_id"] == 1, "order_date"] = "2026-11-04"
    version = lake.replace_silver(replacement, "order_id IN (1, 2)")

    assert lake.silver_feed(version) == FeedSummary(2, 2, (1,), ("2026-11-03", "2026-11-04"))


def test_feed_includes_inserted_and_removed_orders(tmp_path: Path) -> None:
    lake = DeltaLake(tmp_path)
    lake.overwrite_silver(
        pd.DataFrame(
            [
                {"order_id": 1, "order_date": "2026-11-03", "status": "shipped"},
                {"order_id": 2, "order_date": "2026-11-03", "status": "shipped"},
            ]
        )
    )
    replacement = pd.DataFrame(
        [
            {"order_id": 2, "order_date": "2026-11-03", "status": "shipped"},
            {"order_id": 3, "order_date": "2026-11-05", "status": "placed"},
        ]
    )
    version = lake.replace_silver(replacement, "order_id IN (1, 2, 3)")

    assert lake.silver_feed(version) == FeedSummary(2, 2, (1, 3), ("2026-11-03", "2026-11-05"))
