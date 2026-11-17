"""Build Silver and Gold, and repair them from a pinned Bronze version."""

from __future__ import annotations

import pandas as pd

from .models import RepairRequest, RowDiff
from .protocols import Lake, StatusRule


def build_silver(changes: pd.DataFrame, rule: StatusRule) -> pd.DataFrame:
    """Latest change per order, by source sequence, with the rule applied."""
    latest = changes.sort_values(["order_id", "seq"]).groupby("order_id").tail(1).copy()
    latest["status"] = latest["raw_status"].map(rule.status)
    return latest[["order_id", "order_date", "status"]].sort_values("order_id").reset_index(drop=True)


def build_gold(silver: pd.DataFrame) -> pd.DataFrame:
    """Order counts by order date and status."""
    gold = silver.groupby(["order_date", "status"]).size().reset_index(name="orders")
    return gold.sort_values(["order_date", "status"]).reset_index(drop=True)


def diff_rows(left: pd.DataFrame, right: pd.DataFrame, key: str = "order_id") -> RowDiff:
    """Compare two tables in both directions, row by row."""
    a, b = left.set_index(key), right.set_index(key)
    shared = a.index.intersection(b.index)
    different = [int(k) for k in shared if not a.loc[k].equals(b.loc[k])]
    return RowDiff(
        only_left=tuple(int(k) for k in a.index.difference(b.index)),
        only_right=tuple(int(k) for k in b.index.difference(a.index)),
        different=tuple(different),
    )


class Medallion:
    """The daily job and the repair share `build_silver` and one rule."""

    def __init__(self, lake: Lake) -> None:
        self.lake = lake
        self.processed_bronze_version = -1  # our stand-in for a checkpoint

    def first_load(self, rule: StatusRule) -> None:
        version = self.lake.bronze_version()
        silver = build_silver(self.lake.read_bronze(version), rule)
        self.lake.overwrite_silver(silver)
        self.lake.overwrite_gold(build_gold(silver))
        self.processed_bronze_version = version

    def daily_run(self, rule: StatusRule) -> int:
        """Process only orders with changes after the saved position. Returns orders touched."""
        version = self.lake.bronze_version()
        if version == self.processed_bronze_version:
            return 0
        everything = self.lake.read_bronze(version)
        before = self.lake.read_bronze(self.processed_bronze_version)
        merged = everything.merge(before, how="left", indicator=True)
        new_keys = sorted(int(k) for k in merged[merged["_merge"] == "left_only"]["order_id"].unique())
        seen = set(before["order_id"])
        rows = build_silver(everything[everything["order_id"].isin(new_keys)], rule)
        scope = f"order_id IN ({', '.join(map(str, new_keys))})"
        if set(new_keys) & seen:
            self.lake.replace_silver(rows, scope)
        else:
            self.lake.append_silver(rows)
        self.lake.overwrite_gold(build_gold(self.lake.read_silver()))
        self.processed_bronze_version = version
        return len(new_keys)

    def repair_silver(self, request: RepairRequest, rule: StatusRule) -> int:
        """Rebuild from the pinned Bronze version and replace only the scope."""
        rebuilt = build_silver(self.lake.read_bronze(request.bronze_version), rule)
        in_scope = rebuilt.query(_to_pandas(request.scope))
        return self.lake.replace_silver(in_scope, request.scope)

    def repair_gold(self, dates: list[str]) -> int:
        """Recompute every group for the affected dates, so emptied groups disappear."""
        silver = self.lake.read_silver()
        groups = build_gold(silver[silver["order_date"].isin(dates)])
        scope = "order_date IN (" + ", ".join(f"'{d}'" for d in dates) + ")"
        return self.lake.replace_gold(groups, scope)

    def full_rebuild(self, bronze_version: int, rule: StatusRule) -> pd.DataFrame:
        """What Silver would be if we rebuilt everything from one Bronze version."""
        return build_silver(self.lake.read_bronze(bronze_version), rule)


def _to_pandas(scope: str) -> str:
    """Translate the small SQL predicates used here into a pandas query."""
    return scope.replace(" AND ", " and ").replace(" IN ", " in ").replace("'", '"')
