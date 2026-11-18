"""Delta Lake tables on local disk, through the open-source `deltalake` library."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

from .models import FeedSummary

SILVER_COLUMNS = ["order_id", "order_date", "status"]
GOLD_COLUMNS = ["order_date", "status", "orders"]


def read_delta_snapshot(table: DeltaTable) -> pd.DataFrame:
    """Read small local snapshots without Arrow's background scan/conversion pools.

    Serial reads avoid an intermittent thread-pool shutdown hang observed in the
    CLI with PyArrow 26.0.1. Related upstream report:
    https://github.com/apache/arrow/issues/50482
    """
    return table.to_pyarrow_dataset().to_table(use_threads=False).to_pandas(use_threads=False)


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
        return read_delta_snapshot(DeltaTable(self.bronze, version=version))

    # ---- Silver: one row per order, change feed enabled ------------
    def read_silver(self, version: int | None = None) -> pd.DataFrame:
        frame = read_delta_snapshot(DeltaTable(self.silver, version=version))
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
        ).to_pandas(use_threads=False)
        deletes = feed[feed["_change_type"].isin(["delete", "update_preimage"])].set_index("order_id")
        inserts = feed[feed["_change_type"].isin(["insert", "update_postimage"])].set_index("order_id")
        columns = [column for column in SILVER_COLUMNS if column != "order_id"]
        changed = sorted(
            int(key)
            for key in set(deletes.index) | set(inserts.index)
            if key not in deletes.index
            or key not in inserts.index
            or not deletes.loc[key, columns].equals(inserts.loc[key, columns])
        )
        dates = set(deletes[deletes.index.isin(changed)]["order_date"])
        dates.update(inserts[inserts.index.isin(changed)]["order_date"])
        return FeedSummary(
            deletes=len(deletes),
            inserts=len(inserts),
            changed_keys=tuple(changed),
            affected_dates=tuple(sorted(dates)),
        )

    def vacuum_silver_now(self) -> int:
        """Remove every data file the current Silver version no longer uses."""
        removed = DeltaTable(self.silver).vacuum(retention_hours=0, dry_run=False, enforce_retention_duration=False)
        return len(removed)

    # ---- Gold: order counts by date and status ---------------------
    def read_gold(self) -> pd.DataFrame:
        frame = read_delta_snapshot(DeltaTable(self.gold))
        return frame[GOLD_COLUMNS].sort_values(["order_date", "status"]).reset_index(drop=True)

    def overwrite_gold(self, rows: pd.DataFrame) -> int:
        write_deltalake(self.gold, _arrow(rows, GOLD_COLUMNS), mode="overwrite")
        return DeltaTable(self.gold).version()

    def replace_gold(self, rows: pd.DataFrame, scope: str) -> int:
        write_deltalake(self.gold, _arrow(rows, GOLD_COLUMNS), mode="overwrite", predicate=scope)
        return DeltaTable(self.gold).version()


def _arrow(rows: pd.DataFrame, columns: list[str]) -> pa.Table:
    return pa.Table.from_pandas(rows[columns].reset_index(drop=True), preserve_index=False)
