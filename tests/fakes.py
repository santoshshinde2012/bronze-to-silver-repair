"""In-memory storage adapter for exercising pipeline contracts without Delta."""

from __future__ import annotations

import sqlite3

import pandas as pd


class MemoryLake:
    """Snapshot reads and scoped writes with the same table semantics as DeltaLake."""

    def __init__(self) -> None:
        self.bronze: list[pd.DataFrame] = []
        self.silver: list[pd.DataFrame] = []
        self.gold: list[pd.DataFrame] = []

    def append_bronze(self, changes: list[dict]) -> int:
        incoming = pd.DataFrame(changes)
        rows = pd.concat([self.bronze[-1], incoming], ignore_index=True) if self.bronze else incoming
        return self._commit(self.bronze, rows)

    def bronze_version(self) -> int:
        return len(self.bronze) - 1

    def read_bronze(self, version: int | None = None) -> pd.DataFrame:
        return self.bronze[-1 if version is None else version].copy()

    def read_silver(self, version: int | None = None) -> pd.DataFrame:
        return self.silver[-1 if version is None else version].copy()

    def silver_version(self) -> int:
        return len(self.silver) - 1

    def read_gold(self) -> pd.DataFrame:
        return self.gold[-1].copy()

    def overwrite_silver(self, rows: pd.DataFrame) -> int:
        return self._commit(self.silver, rows.sort_values("order_id"))

    def append_silver(self, rows: pd.DataFrame) -> int:
        return self.overwrite_silver(pd.concat([self.read_silver(), rows], ignore_index=True))

    def replace_silver(self, rows: pd.DataFrame, scope: str) -> int:
        return self.overwrite_silver(self._replace(self.read_silver(), rows, scope))

    def overwrite_gold(self, rows: pd.DataFrame) -> int:
        return self._commit(self.gold, rows.sort_values(["order_date", "status"]))

    def replace_gold(self, rows: pd.DataFrame, scope: str) -> int:
        return self.overwrite_gold(self._replace(self.read_gold(), rows, scope))

    @staticmethod
    def _commit(versions: list[pd.DataFrame], rows: pd.DataFrame) -> int:
        versions.append(rows.reset_index(drop=True).copy())
        return len(versions) - 1

    @staticmethod
    def _replace(current: pd.DataFrame, rows: pd.DataFrame, scope: str) -> pd.DataFrame:
        if len(MemoryLake._matching_indices(rows, scope)) != len(rows):
            raise ValueError("Replacement row is outside the scope.")
        keep = current.drop(MemoryLake._matching_indices(current, scope))
        return pd.concat([keep, rows], ignore_index=True)

    @staticmethod
    def _matching_indices(rows: pd.DataFrame, scope: str) -> pd.Index:
        # Use SQL independently of the pipeline's limited pandas translation.
        with sqlite3.connect(":memory:") as connection:
            rows.to_sql("records", connection, index=False)
            positions = [row[0] for row in connection.execute(f"SELECT rowid - 1 FROM records WHERE {scope}")]
        return rows.iloc[positions].index
