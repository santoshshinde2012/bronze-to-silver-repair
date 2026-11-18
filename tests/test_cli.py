"""Verify that the demo and inspector finish as standalone commands."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


def test_demo_and_inspector_exit_cleanly(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    # Redirect only the demo's composition root, keeping its real CLI entrypoint
    # and ensuring this regression never recreates the repository's lake.
    program = (
        "from pathlib import Path; import sys; "
        "from src import repair_demo; "
        "repair_demo.CODE_ROOT = Path(sys.argv[1]); repair_demo.main()"
    )
    demo = subprocess.run(
        [sys.executable, "-c", program, str(tmp_path)],
        cwd=root,
        capture_output=True,
        text=True,
        timeout=20,
        check=True,
    )
    assert "matches full rebuild: True" in demo.stdout
    output = tmp_path / "report"
    subprocess.run(
        [
            sys.executable,
            "-m",
            "src.inspection",
            "--lake",
            str(tmp_path / "lake"),
            "--output",
            str(output),
        ],
        cwd=root,
        capture_output=True,
        text=True,
        timeout=20,
        check=True,
    )
    snapshot = json.loads((output / "tables.json").read_text())
    assert snapshot["checks"]["silver_matches_corrected_bronze"] is True
    assert snapshot["checks"]["gold_matches_silver"] is True
