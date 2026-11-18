"""Value objects for the repair example."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class RepairRequest:
    """What the article asks us to write down before touching a table."""

    reason: str
    bronze_version: int
    scope: str  # predicate over Silver columns, e.g. "order_id IN (2, 5, 7)"


@dataclass(frozen=True)
class RowDiff:
    """Rows that differ between two versions of the same table, by key."""

    only_left: tuple[int, ...]
    only_right: tuple[int, ...]
    different: tuple[int, ...]

    @property
    def same(self) -> bool:
        return not (self.only_left or self.only_right or self.different)


@dataclass(frozen=True)
class FeedSummary:
    """What the change feed recorded for one commit."""

    deletes: int
    inserts: int
    changed_keys: tuple[int, ...]
    affected_dates: tuple[str, ...] = ()
