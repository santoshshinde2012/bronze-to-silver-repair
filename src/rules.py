"""Status rules. The faulty one is the bug the article repairs."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class FaultyStatusRule:
    """The bug: a canceled order is mapped to 'shipped'."""

    name: str = "faulty"

    def status(self, raw_status: str) -> str:
        return {"PLACED": "placed", "SHIPPED": "shipped", "CANCELLED": "shipped"}[raw_status]


@dataclass(frozen=True)
class CorrectedStatusRule:
    """The reviewed fix: a canceled order is 'canceled'."""

    name: str = "corrected"

    def status(self, raw_status: str) -> str:
        return {"PLACED": "placed", "SHIPPED": "shipped", "CANCELLED": "canceled"}[raw_status]
