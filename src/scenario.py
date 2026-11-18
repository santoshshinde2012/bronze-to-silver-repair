"""The article's repair, step by step, on a lake. Returns every result the article quotes.

Steps follow the article: the rerun, the date-range repair, the full-rebuild check,
the key repair, the Gold repair, and the vacuum.
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any

import deltalake
import pandas as pd
from deltalake.exceptions import DeltaError

from .models import FeedSummary, RepairRequest
from .pipeline import Medallion, build_gold, diff_rows
from .repository import DeltaLake
from .rules import CorrectedStatusRule, FaultyStatusRule

CODE_ROOT = Path(__file__).resolve().parent.parent
SEED_FILE = CODE_ROOT / "data" / "order_changes.json"
DATE_SCOPE = "order_date >= '2026-11-03' AND order_date <= '2026-11-05'"
KEY_SCOPE = "order_id IN (2, 5, 7)"


def run(root: Path, seed_file: Path = SEED_FILE) -> dict[str, Any]:
    """Execute every step and return the observed results."""
    if root.exists():
        shutil.rmtree(root)
    seed = json.loads(seed_file.read_text(encoding="utf-8"))
    lake = DeltaLake(root)
    job = Medallion(lake, lake, lake)
    faulty, corrected = FaultyStatusRule(), CorrectedStatusRule()
    out: dict[str, Any] = {"deltalake": deltalake.__version__}

    # 1. Three Bronze commits, then Silver and Gold built with the faulty rule.
    for batch in seed["batches"]:
        lake.append_bronze(batch)
    job.first_load(faulty)
    pinned = lake.bronze_version()
    out["pinned_bronze_version"] = pinned
    out["silver_with_bug"] = _statuses(lake.read_silver())

    # 2. Deploy the corrected rule and run the daily job again. New data also arrives.
    out["rerun_orders_touched"] = job.daily_run(corrected)
    lake.append_bronze(seed["arrives_after_pin"])
    out["next_run_orders_touched"] = job.daily_run(corrected)
    out["silver_after_reruns"] = _statuses(lake.read_silver())

    # 3. First repair: the date range named in the incident.
    before_repair = lake.silver_version()
    first = RepairRequest("canceled mapped to shipped", pinned, DATE_SCOPE)
    v_first = job.repair_silver(first, corrected)
    out["silver_after_date_repair"] = _statuses(lake.read_silver())
    feed = lake.silver_feed(v_first)
    out["feed_date_repair"] = _feed(feed)

    # 4. Repeat the same repair and compare every business column.
    before_repeat = lake.read_silver()
    job.repair_silver(first, corrected)
    out["repeat_changes_rows"] = not lake.read_silver().equals(before_repeat)
    out["silver_rows_after_repeat"] = len(lake.read_silver())

    # 5. A row outside the scope is refused.
    try:
        outside = lake.read_silver().head(1).assign(order_id=99, order_date="2026-12-01")
        lake.replace_silver(outside, DATE_SCOPE)
        out["outside_row"] = "accepted"
    except DeltaError as exc:
        out["outside_row"] = f"refused ({type(exc).__name__})"

    # 6. Compare with a full rebuild from the latest Bronze. This catches the missed order.
    reference = job.full_rebuild(lake.bronze_version(), corrected)
    gap = diff_rows(lake.read_silver(), reference)
    out["date_repair_vs_full_rebuild"] = {
        "different": list(gap.different),
        "missing": list(gap.only_right),
        "extra": list(gap.only_left),
    }

    # 7. Second repair: scope by the affected keys.
    second = RepairRequest("canceled mapped to shipped", pinned, KEY_SCOPE)
    v_second = job.repair_silver(second, corrected)
    feed2 = lake.silver_feed(v_second)
    out["feed_key_repair"] = _feed(feed2)
    out["key_repair_vs_full_rebuild_same"] = diff_rows(lake.read_silver(), reference).same
    out["order_8_rows"] = int((lake.read_silver()["order_id"] == 8).sum())

    # 8. Silver is right, Gold is not yet. Then repair the affected Gold dates.
    expected_gold = build_gold(lake.read_silver())
    out["gold_before_gold_repair"] = _groups(lake.read_gold())
    out["gold_matches_silver_before"] = lake.read_gold().equals(expected_gold)
    affected_dates = sorted(set(feed.affected_dates) | set(feed2.affected_dates))
    out["gold_repair_dates"] = affected_dates
    job.repair_gold(affected_dates)
    out["gold_after_gold_repair"] = _groups(lake.read_gold())
    out["gold_matches_silver_after"] = lake.read_gold().equals(expected_gold)

    # 9. Rollback limit: after a vacuum, the pre-repair Silver version cannot be read.
    out["pre_repair_readable_before_vacuum"] = _readable(lake, before_repair)
    out["files_vacuumed"] = lake.vacuum_silver_now()
    out["pre_repair_readable_after_vacuum"] = _readable(lake, before_repair)
    out["bronze_pinned_readable_at_end"] = len(lake.read_bronze(pinned))
    return out


def _feed(summary: FeedSummary) -> dict[str, Any]:
    return {"deletes": summary.deletes, "inserts": summary.inserts, "changed": list(summary.changed_keys)}


def _statuses(silver: pd.DataFrame) -> dict[int, str]:
    return {int(r.order_id): r.status for r in silver.itertuples()}


def _groups(gold: pd.DataFrame) -> dict[str, int]:
    return {f"{r.order_date} {r.status}": int(r.orders) for r in gold.itertuples()}


def _readable(lake: DeltaLake, version: int) -> bool:
    try:
        lake.read_silver(version)
        return True
    except FileNotFoundError:
        return False
