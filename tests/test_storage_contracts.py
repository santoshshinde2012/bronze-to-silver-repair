"""Run the same pipeline behavior against real Delta and in-memory adapters."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest
from deltalake.exceptions import DeltaError

from src.models import RepairRequest
from src.pipeline import Medallion, build_gold, diff_rows
from src.repository import DeltaLake
from src.rules import CorrectedStatusRule, FaultyStatusRule
from tests.fakes import MemoryLake

INITIAL = [
    {"order_id": 1, "seq": 1, "order_date": "2026-11-03", "raw_status": "CANCELLED"},
    {"order_id": 2, "seq": 1, "order_date": "2026-11-04", "raw_status": "SHIPPED"},
]


@pytest.fixture(params=["memory", "delta"])
def lake(request: pytest.FixtureRequest, tmp_path: Path) -> MemoryLake | DeltaLake:
    return MemoryLake() if request.param == "memory" else DeltaLake(tmp_path / "lake")


def test_incremental_run_updates_existing_orders_and_inserts_new_orders(lake: MemoryLake | DeltaLake) -> None:
    lake.append_bronze(INITIAL)
    job = Medallion(lake, lake, lake)
    job.first_load(CorrectedStatusRule())
    lake.append_bronze(
        [
            {"order_id": 2, "seq": 2, "order_date": "2026-11-04", "raw_status": "CANCELLED"},
            {"order_id": 3, "seq": 1, "order_date": "2026-11-05", "raw_status": "PLACED"},
        ]
    )

    assert job.daily_run(CorrectedStatusRule()) == 2
    assert lake.read_silver().set_index("order_id")["status"].to_dict() == {1: "canceled", 2: "canceled", 3: "placed"}
    pd.testing.assert_frame_equal(lake.read_gold(), build_gold(lake.read_silver()))


def test_duplicate_only_commit_advances_checkpoint_without_writing_tables(lake: MemoryLake | DeltaLake) -> None:
    lake.append_bronze(INITIAL)
    job = Medallion(lake, lake, lake)
    job.first_load(CorrectedStatusRule())
    version = lake.silver_version()
    gold_before = lake.read_gold()
    lake.append_bronze(INITIAL)

    assert job.daily_run(CorrectedStatusRule()) == 0
    assert job.processed_bronze_version == lake.bronze_version()
    assert lake.silver_version() == version
    pd.testing.assert_frame_equal(lake.read_gold(), gold_before)


def test_pinned_repair_preserves_late_orders_and_repairs_emptied_gold_groups(lake: MemoryLake | DeltaLake) -> None:
    pinned = lake.append_bronze(INITIAL)
    job = Medallion(lake, lake, lake)
    job.first_load(FaultyStatusRule())
    lake.append_bronze([{"order_id": 3, "seq": 1, "order_date": "2026-11-05", "raw_status": "PLACED"}])
    job.daily_run(CorrectedStatusRule())
    before = lake.read_silver()
    request = RepairRequest("fix cancellation", pinned, "order_id IN (1)")

    job.repair_silver(request, CorrectedStatusRule())
    first_repair = lake.read_silver()
    job.repair_silver(request, CorrectedStatusRule())
    assert diff_rows(before, first_repair).different == (1,)
    pd.testing.assert_frame_equal(first_repair, lake.read_silver())
    assert lake.read_silver().set_index("order_id")["status"].to_dict() == {1: "canceled", 2: "shipped", 3: "placed"}
    job.repair_gold(["2026-11-03"])
    pd.testing.assert_frame_equal(lake.read_gold(), build_gold(lake.read_silver()))


def test_replacement_removes_scope_when_the_rebuilt_scope_is_empty(lake: MemoryLake | DeltaLake) -> None:
    lake.append_bronze(INITIAL)
    Medallion(lake, lake, lake).first_load(CorrectedStatusRule())
    original = lake.read_silver()
    before = lake.silver_version()

    lake.replace_silver(original.iloc[:0], "order_id IN (1)")

    assert lake.read_silver()["order_id"].tolist() == [2]
    pd.testing.assert_frame_equal(lake.read_silver(before), original)


def test_replacement_rejects_outside_rows_without_changing_the_table(lake: MemoryLake | DeltaLake) -> None:
    lake.append_bronze(INITIAL)
    Medallion(lake, lake, lake).first_load(CorrectedStatusRule())
    original = lake.read_silver()
    outside = original.head(1).assign(order_id=3)
    before = lake.silver_version()

    with pytest.raises((DeltaError, ValueError)):
        lake.replace_silver(outside, "order_id IN (1)")

    assert lake.silver_version() == before
    pd.testing.assert_frame_equal(lake.read_silver(), original)


def test_daily_run_requires_an_initialized_checkpoint(lake: MemoryLake | DeltaLake) -> None:
    job = Medallion(lake, lake, lake)
    with pytest.raises(RuntimeError, match="first_load"):
        job.daily_run(CorrectedStatusRule())


def test_gold_repair_rejects_an_empty_scope(lake: MemoryLake | DeltaLake) -> None:
    job = Medallion(lake, lake, lake)
    with pytest.raises(ValueError, match="at least one"):
        job.repair_gold([])
