"""Interfaces the pipeline depends on. The pipeline never imports `deltalake`."""

from __future__ import annotations

from typing import Protocol

import pandas as pd


class StatusRule(Protocol):
    """Maps a raw source status to the cleaned Silver status."""

    def status(self, raw_status: str) -> str: ...


class BronzeReader(Protocol):
    """Read source history at a stable, zero-based version."""

    def bronze_version(self) -> int: ...

    def read_bronze(self, version: int | None = None) -> pd.DataFrame: ...


class SilverStore(Protocol):
    """Persist one row per order; scoped replacements preserve rows outside the scope.

    Writes return the committed version. A replacement rejects supplied rows that
    do not match its predicate, and replaces matching rows even when empty.
    """

    def read_silver(self, version: int | None = None) -> pd.DataFrame: ...

    def overwrite_silver(self, rows: pd.DataFrame) -> int: ...

    def append_silver(self, rows: pd.DataFrame) -> int: ...

    def replace_silver(self, rows: pd.DataFrame, scope: str) -> int: ...


class GoldStore(Protocol):
    """Persist order counts; scoped replacements also remove emptied groups."""

    def overwrite_gold(self, rows: pd.DataFrame) -> int: ...

    def replace_gold(self, rows: pd.DataFrame, scope: str) -> int: ...


class Lake(BronzeReader, SilverStore, GoldStore, Protocol):
    """Composite table contract for adapters offering all three capabilities."""

    def read_gold(self) -> pd.DataFrame: ...
