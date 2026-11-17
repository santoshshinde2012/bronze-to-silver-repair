"""Interfaces the pipeline depends on. The pipeline never imports `deltalake`."""

from __future__ import annotations

from typing import Protocol

import pandas as pd


class StatusRule(Protocol):
    """Maps a raw source status to the cleaned Silver status."""

    @property
    def name(self) -> str: ...

    def status(self, raw_status: str) -> str: ...


class Lake(Protocol):
    """The table operations a repair needs. Implemented with Delta tables."""

    def bronze_version(self) -> int: ...

    def read_bronze(self, version: int | None = None) -> pd.DataFrame: ...

    def read_silver(self, version: int | None = None) -> pd.DataFrame: ...

    def read_gold(self) -> pd.DataFrame: ...

    def overwrite_silver(self, rows: pd.DataFrame) -> int: ...

    def append_silver(self, rows: pd.DataFrame) -> int: ...

    def replace_silver(self, rows: pd.DataFrame, scope: str) -> int: ...

    def overwrite_gold(self, rows: pd.DataFrame) -> int: ...

    def replace_gold(self, rows: pd.DataFrame, scope: str) -> int: ...
