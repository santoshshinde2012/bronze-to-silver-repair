"""Composition root: run the repair on local Delta tables and print what happened.

Run from the `code/` folder:  python -m src.repair_demo
"""

from __future__ import annotations

from typing import Any

from .scenario import CODE_ROOT, run


def report(r: dict[str, Any]) -> list[str]:
    """The lines the article quotes, in order."""
    return [
        f"deltalake {r['deltalake']} on local disk; not a Databricks runtime",
        f"1. Bronze pinned at version {r['pinned_bronze_version']}. Silver with the bug: {r['silver_with_bug']}",
        f"2. Rerun with the corrected rule touched {r['rerun_orders_touched']} orders; "
        f"the next run touched {r['next_run_orders_touched']}.",
        f"   Silver afterwards: {r['silver_after_reruns']}",
        f"3. Date-range repair. Silver: {r['silver_after_date_repair']}",
        f"   Change feed for that commit: {r['feed_date_repair']}",
        f"4. Same repair again changed rows: {r['repeat_changes_rows']}; "
        f"Silver has {r['silver_rows_after_repeat']} rows.",
        f"5. Row outside the scope: {r['outside_row']}",
        f"6. Date-range repair vs full rebuild: {r['date_repair_vs_full_rebuild']}",
        f"7. Key repair. Change feed: {r['feed_key_repair']}; "
        f"matches full rebuild: {r['key_repair_vs_full_rebuild_same']}; rows for order 8: {r['order_8_rows']}",
        f"8. Gold before its repair matches Silver: {r['gold_matches_silver_before']}: {r['gold_before_gold_repair']}",
        f"   Gold after its repair matches Silver: {r['gold_matches_silver_after']}: {r['gold_after_gold_repair']}",
        f"9. Pre-repair Silver readable before vacuum: {r['pre_repair_readable_before_vacuum']}; "
        f"files removed: {r['files_vacuumed']}; readable after: {r['pre_repair_readable_after_vacuum']}",
        f"   Pinned Bronze version still readable at the end: {r['bronze_pinned_readable_at_end']} rows",
    ]


def main() -> None:
    for line in report(run(CODE_ROOT / "lake")):
        print(line)


if __name__ == "__main__":
    main()
