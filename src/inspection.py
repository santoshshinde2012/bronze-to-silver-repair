"""Export a read-only, offline view of the actual local Delta table snapshots."""

from __future__ import annotations

import argparse
import html
import json
import platform
from pathlib import Path
from typing import Any

import deltalake
import pandas as pd
import pyarrow as pa
from deltalake import DeltaTable
from deltalake.exceptions import DeltaError

from .pipeline import build_gold, build_silver, diff_rows
from .repository import read_delta_snapshot
from .rules import CorrectedStatusRule

TABLES = {
    "bronze": ("bronze_order_changes", ["order_id", "seq"]),
    "silver": ("silver_orders", ["order_id"]),
    "gold": ("gold_daily_order_totals", ["order_date", "status"]),
}


def inspect_lake(root: Path, bronze_version: int = 2) -> dict[str, Any]:
    """Read versioned snapshots without running the scenario or modifying the lake.

    Each table's version is captured before reading its rows. This does not create
    a transaction across the three tables; stop writers for a consistent export.
    """
    frames: dict[str, pd.DataFrame] = {}
    tables: dict[str, Any] = {}
    for layer, (name, sort_columns) in TABLES.items():
        table = DeltaTable(str(root / name))
        frame = read_delta_snapshot(table).sort_values(sort_columns).reset_index(drop=True)
        frames[layer] = frame
        tables[layer] = {
            "name": name,
            "version": table.version(),
            "row_count": len(frame),
            "records": json.loads(frame.to_json(orient="records")),
        }

    pinned: dict[str, Any] = {"version": bronze_version, "readable": False, "row_count": None}
    try:
        pinned_frame = read_delta_snapshot(DeltaTable(str(root / TABLES["bronze"][0]), version=bronze_version))
        pinned.update(readable=True, row_count=len(pinned_frame))
    except (DeltaError, OSError):
        # The final tables remain inspectable if this optional historical version
        # is absent or its files have already expired.
        pass

    duplicate_orders = int(frames["silver"]["order_id"].duplicated().sum())
    reference = build_silver(frames["bronze"], CorrectedStatusRule())
    checks = {
        "silver_matches_corrected_bronze": duplicate_orders == 0 and diff_rows(frames["silver"], reference).same,
        "silver_duplicate_order_ids": duplicate_orders,
        "gold_matches_silver": frames["gold"].equals(build_gold(frames["silver"])),
        "gold_total_orders": int(frames["gold"]["orders"].sum()),
    }
    return {
        "source": root.as_posix(),
        "data": "Synthetic order events stored in real local Delta tables",
        "runtime": {
            "python": platform.python_version(),
            "deltalake": deltalake.__version__,
            "pandas": pd.__version__,
            "pyarrow": pa.__version__,
        },
        "tables": tables,
        "pinned_bronze": pinned,
        "checks": checks,
    }


def _table(records: list[dict[str, Any]], columns: list[str]) -> str:
    headings = "".join(f'<th scope="col">{html.escape(column)}</th>' for column in columns)
    rows = []
    for record in records:
        cells = "".join(f"<td>{html.escape(str(record[column]))}</td>" for column in columns)
        row_class = ' class="cancellation"' if record.get("status") == "canceled" else ""
        rows.append(f"<tr{row_class}>{cells}</tr>")
    return f"<table><thead><tr>{headings}</tr></thead><tbody>{''.join(rows)}</tbody></table>"


def render_report(snapshot: dict[str, Any]) -> str:
    """Render a technical evidence sheet directly from the exported table values."""
    tables, checks, pinned = snapshot["tables"], snapshot["checks"], snapshot["pinned_bronze"]
    silver = _table(tables["silver"]["records"], ["order_id", "order_date", "status"])
    gold = _table(tables["gold"]["records"], ["order_date", "status", "orders"])
    passed = checks["silver_matches_corrected_bronze"] and checks["gold_matches_silver"]
    conclusion = "VERIFIED" if passed else "REVIEW REQUIRED"
    result_class = "match" if passed else "mismatch"
    validations = [
        (
            "Silver vs. corrected Bronze rebuild",
            "Equal",
            "Equal" if checks["silver_matches_corrected_bronze"] else "Different",
        ),
        ("Gold vs. Silver aggregation", "Equal", "Equal" if checks["gold_matches_silver"] else "Different"),
        ("Duplicate Silver order IDs", "0", str(checks["silver_duplicate_order_ids"])),
    ]
    validation_rows = "".join(
        f"<tr><td>{label}</td><td>{expected}</td>"
        f'<td class="{"match" if expected == observed else "mismatch"}">{observed}</td></tr>'
        for label, expected, observed in validations
    )
    inventory_rows = "".join(
        f"<tr><td>{table['name']}</td><td>v{table['version']}</td><td>{table['row_count']}</td></tr>"
        for table in tables.values()
    )
    pinned_count = str(pinned["row_count"]) if pinned["readable"] else "Unavailable"
    pinned_detail = f"v{pinned['version']} / {pinned_count} events"
    runtime = " · ".join(f"{name} {version}" for name, version in snapshot["runtime"].items())
    canceled_count = sum(row["status"] == "canceled" for row in tables["silver"]["records"])
    silver_detail = f"{tables['silver']['row_count']} orders · {canceled_count} canceled"
    gold_detail = f"{tables['gold']['row_count']} groups · {checks['gold_total_orders']} orders counted"
    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Delta Lake repair — persisted result</title>
<style>
* {{ box-sizing: border-box; }}
body {{ margin: 0; color: #182b3d; background: #fff;
font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Arial, sans-serif; }}
main {{ max-width: 1440px; margin: auto; padding: 48px 56px 40px; }}
.kicker {{ display: flex; justify-content: space-between; color: #486176;
font: 600 13px ui-monospace, "SFMono-Regular", Consolas, monospace; letter-spacing: .8px; }}
header {{ padding-bottom: 25px; border-bottom: 2px solid #182b3d; }}
h1 {{ margin: 20px 0 12px; font-size: 42px; font-weight: 650; letter-spacing: -1.1px; }}
header p {{ margin: 0; font-size: 17px; color: #526474; line-height: 1.55; }}
.context {{ display: flex; justify-content: space-between; gap: 24px; padding: 17px 0;
font-size: 14px; color: #526474; border-bottom: 1px solid #d8e0e6; }}
.context strong {{ font-weight: 600; color: #182b3d; }}
code, td {{ font-family: ui-monospace, "SFMono-Regular", Menlo, Consolas, monospace; }}
.tables {{ display: grid; grid-template-columns: 1.05fr 1fr; gap: 48px; margin-top: 32px; }}
.table-heading {{ display: flex; align-items: baseline; justify-content: space-between; gap: 12px; }}
h2 {{ margin: 0; font-size: 23px; font-weight: 650; letter-spacing: -.4px; }}
.table-heading span {{ color: #526474; font-size: 14px; white-space: nowrap; }}
.table-caption {{ margin: 9px 0 18px; color: #526474; font-size: 14px; }}
table {{ border-collapse: collapse; width: 100%; text-align: left; }}
th {{ padding: 13px 15px; border-top: 1px solid #182b3d; border-bottom: 1px solid #9fadb8;
font: 600 13px ui-monospace, "SFMono-Regular", Consolas, monospace; color: #43586a; }}
td {{ padding: 13px 15px; border-bottom: 1px solid #e1e7ec; font-size: 16px; line-height: 1.4; }}
.tables tbody tr:last-child td {{ border-bottom-color: #9fadb8; }}
.cancellation td {{ background: #f1f6fb; color: #143b60; }}
.cancellation td:first-child {{ box-shadow: inset 3px 0 #245f94; }}
.table-note {{ margin: 13px 0 0; color: #526474; font-size: 13px; line-height: 1.5; }}
.evidence {{ display: grid; grid-template-columns: 1.05fr 1fr; gap: 48px;
margin-top: 32px; padding-top: 25px; border-top: 2px solid #182b3d; }}
.evidence h2 {{ margin-bottom: 17px; font-size: 18px; }}
.evidence th {{ padding: 9px 0; border-top: none; font-size: 12px; }}
.evidence td {{ padding: 12px 0; font-size: 13px; }}
.evidence td:first-child {{ font-family: inherit; }}
.inventory td:first-child {{ font-family: ui-monospace, "SFMono-Regular", Consolas, monospace; font-size: 12px; }}
.match {{ color: #245f94; font-weight: 600; }}
.mismatch {{ color: #a13628; font-weight: 600; }}
footer {{ margin-top: 26px; padding-top: 18px; border-top: 1px solid #d8e0e6;
color: #526474; font-size: 12px; line-height: 1.7; }}
footer p {{ margin: 0 0 4px; }}
footer .method {{ color: #182b3d; }}
a {{ color: #245f94; }}
@media (max-width: 900px) {{ main {{ padding: 28px; }} h1 {{ font-size: 32px; }}
.tables, .evidence {{ grid-template-columns: 1fr; gap: 28px; }}
.context, .kicker {{ flex-wrap: wrap; gap: 10px; }} }}
</style></head><body><main>
<header><div class="kicker"><span>BRONZE TO SILVER REPAIR / RESULT EVIDENCE</span>
<span class="{result_class}">{conclusion}</span></div>
<h1>Delta Lake repair — persisted result</h1>
<p>Current table snapshots, compared with a corrected Bronze rebuild and the Silver aggregation.</p></header>
<div class="context"><span><strong>Source</strong> <code>{html.escape(snapshot["source"])}/</code></span>
<span><strong>Pinned Bronze</strong> <code>{pinned_detail}</code></span>
<span><strong>Read mode</strong> versioned Delta snapshots</span></div>
<section class="tables">
<article><div class="table-heading"><h2>Silver / order state</h2><span>{silver_detail}</span></div>
<p class="table-caption"><code>silver_orders</code> · version {tables["silver"]["version"]}</p>{silver}
<p class="table-note">Blue rules identify stored cancellations; all rows are shown.</p></article>
<article><div class="table-heading"><h2>Gold / daily totals</h2><span>{gold_detail}</span></div>
<p class="table-caption"><code>gold_daily_order_totals</code> · version {tables["gold"]["version"]}</p>{gold}
<p class="table-note">Grouped by <code>order_date</code> and <code>status</code>; all groups are shown.</p></article>
</section>
<section class="evidence">
<article><h2>Verification results</h2><table><thead><tr><th scope="col">Comparison</th>
<th scope="col">Expected</th><th scope="col">Observed</th></tr></thead>
<tbody>{validation_rows}</tbody></table></article>
<article class="inventory"><h2>Snapshot provenance</h2><table><thead><tr><th scope="col">Delta table</th>
<th scope="col">Version</th><th scope="col">Rows</th></tr></thead><tbody>{inventory_rows}</tbody></table></article>
</section>
<footer><p class="method">{html.escape(snapshot["data"])}. Inspection does not modify the lake.</p>
<p>{html.escape(runtime)}</p><p>Evidence: <a href="tables.json">tables.json</a> ·
Reproduce: <code>python -m src.inspection --lake lake --output docs/results</code></p></footer>
</main></body></html>
"""


def export_report(root: Path, output: Path, bronze_version: int = 2) -> dict[str, Any]:
    """Validate and read the lake before creating report artifacts."""
    snapshot = inspect_lake(root, bronze_version)
    document = render_report(snapshot)
    output.mkdir(parents=True, exist_ok=True)
    (output / "tables.json").write_text(json.dumps(snapshot, indent=2) + "\n", encoding="utf-8")
    (output / "index.html").write_text(document, encoding="utf-8")
    return snapshot


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lake", type=Path, default=Path("lake"), help="Existing demo lake; never recreated")
    parser.add_argument("--output", type=Path, default=Path("docs/results"), help="Directory for HTML and JSON")
    parser.add_argument("--bronze-version", type=int, default=2, help="Historical repair input to inspect (default: 2)")
    args = parser.parse_args()
    if args.bronze_version < 0:
        parser.error("--bronze-version must be nonnegative")
    try:
        export_report(args.lake, args.output, args.bronze_version)
    except (DeltaError, OSError, ValueError, KeyError) as exc:
        parser.exit(1, f"Cannot inspect lake at {args.lake}: {exc}\nRun the demo first or choose an existing lake.\n")
    print(f"Exported actual Delta snapshots to {args.output / 'index.html'} and {args.output / 'tables.json'}")


if __name__ == "__main__":
    main()
