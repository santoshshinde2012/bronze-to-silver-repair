"""Delta Lake tables on local disk, through the open-source `deltalake` library."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

from .models import FeedSummary

SILVER_COLUMNS = ["order_id", "order_date", "status"]
GOLD_COLUMNS = ["order_date", "status", "orders"]


class DeltaLake:
    """Bronze, Silver and Gold as three Delta tables under one folder."""

    def __init__(self, root: Path) -> None:
        self.bronze = str(root / "bronze_order_changes")
        self.silver = str(root / "silver_orders")
        self.gold = str(root / "gold_daily_order_totals")

    # ---- Bronze: append only -------------------------------------
    def append_bronze(self, changes: list[dict]) -> int:
        write_deltalake(self.bronze, pa.Table.from_pylist(changes), mode="append")
        return self.bronze_version()

    def bronze_version(self) -> int:
        return DeltaTable(self.bronze).version()

    def read_bronze(self, version: int | None = None) -> pd.DataFrame:
        return DeltaTable(self.bronze, version=version).to_pandas()

    # ---- Silver: one row per order, change feed enabled ------------
    def read_silver(self, version: int | None = None) -> pd.DataFrame:
        frame = DeltaTable(self.silver, version=version).to_pandas()
        return frame[SILVER_COLUMNS].sort_values("order_id").reset_index(drop=True)

    def silver_version(self) -> int:
        return DeltaTable(self.silver).version()

    def overwrite_silver(self, rows: pd.DataFrame) -> int:
        write_deltalake(
            self.silver,
            _arrow(rows, SILVER_COLUMNS),
            mode="overwrite",
            configuration={"delta.enableChangeDataFeed": "true"},
        )
        return self.silver_version()

    def append_silver(self, rows: pd.DataFrame) -> int:
        write_deltalake(self.silver, _arrow(rows, SILVER_COLUMNS), mode="append")
        return self.silver_version()

    def replace_silver(self, rows: pd.DataFrame, scope: str) -> int:
        """Replace the rows that match `scope` in one commit."""
        write_deltalake(self.silver, _arrow(rows, SILVER_COLUMNS), mode="overwrite", predicate=scope)
        return self.silver_version()

    def silver_feed(self, version: int) -> FeedSummary:
        """Summarise the change feed of one Silver commit."""
        feed = pa.table(
            DeltaTable(self.silver).load_cdf(starting_version=version, ending_version=version).read_all()
        ).to_pandas()
        deletes = feed[feed["_change_type"] == "delete"].set_index("order_id")["status"]
        inserts = feed[feed["_change_type"] == "insert"].set_index("order_id")["status"]
        changed = sorted(
            int(key) for key in set(deletes.index) | set(inserts.index) if deletes.get(key) != inserts.get(key)
        )
        return FeedSummary(deletes=len(deletes), inserts=len(inserts), changed_keys=tuple(changed))

    def vacuum_silver_now(self) -> int:
        """Remove every data file the current Silver version no longer uses."""
        removed = DeltaTable(self.silver).vacuum(retention_hours=0, dry_run=False, enforce_retention_duration=False)
        return len(removed)

    # ---- Gold: order counts by date and status ---------------------
    def read_gold(self) -> pd.DataFrame:
        frame = DeltaTable(self.gold).to_pandas()
        return frame[GOLD_COLUMNS].sort_values(["order_date", "status"]).reset_index(drop=True)

    def overwrite_gold(self, rows: pd.DataFrame) -> int:
        write_deltalake(self.gold, _arrow(rows, GOLD_COLUMNS), mode="overwrite")
        return DeltaTable(self.gold).version()

    def replace_gold(self, rows: pd.DataFrame, scope: str) -> int:
        write_deltalake(self.gold, _arrow(rows, GOLD_COLUMNS), mode="overwrite", predicate=scope)
        return DeltaTable(self.gold).version()


def _arrow(rows: pd.DataFrame, columns: list[str]) -> pa.Table:
    return pa.Table.from_pandas(rows[columns].reset_index(drop=True), preserve_index=False)
