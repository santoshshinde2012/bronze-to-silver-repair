"""Lock every result the article quotes from the local run."""

from __future__ import annotations


def test_rerun_with_the_fixed_rule_leaves_old_rows_wrong(result):
    assert result["rerun_orders_touched"] == 0
    assert result["next_run_orders_touched"] == 1
    assert [result["silver_after_reruns"][k] for k in (2, 5, 7)] == ["shipped"] * 3


def test_date_range_repair_fixes_only_orders_inside_the_range(result):
    silver = result["silver_after_date_repair"]
    assert (silver[2], silver[5], silver[7]) == ("canceled", "canceled", "shipped")


def test_change_feed_records_every_row_in_scope_not_only_changed_rows(result):
    assert result["feed_date_repair"] == {"deletes": 6, "inserts": 6, "changed": [2, 5]}
    assert result["feed_key_repair"] == {"deletes": 3, "inserts": 3, "changed": [7]}


def test_repeating_the_repair_changes_nothing(result):
    assert result["repeat_changes_rows"] is False
    assert result["silver_rows_after_repeat"] == 8


def test_row_outside_the_scope_is_refused(result):
    assert result["outside_row"].startswith("refused")


def test_full_rebuild_comparison_catches_the_late_order(result):
    assert result["date_repair_vs_full_rebuild"] == {"different": [7], "missing": [], "extra": []}
    assert result["key_repair_vs_full_rebuild_same"] is True


def test_data_that_arrived_after_the_pin_is_kept_once(result):
    assert result["order_8_rows"] == 1


def test_gold_is_wrong_until_its_own_repair_and_emptied_groups_disappear(result):
    assert result["gold_repair_dates"] == ["2026-11-01", "2026-11-03", "2026-11-04"]
    assert result["gold_matches_silver_before"] is False
    assert result["gold_matches_silver_after"] is True
    assert "2026-11-01 shipped" not in result["gold_after_gold_repair"]
    assert result["gold_after_gold_repair"]["2026-11-03 shipped"] == 2
    assert result["gold_after_gold_repair"]["2026-11-03 canceled"] == 1


def test_vacuum_removes_the_way_back(result):
    assert result["pre_repair_readable_before_vacuum"] is True
    assert result["files_vacuumed"] > 0
    assert result["pre_repair_readable_after_vacuum"] is False
