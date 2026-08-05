#!/usr/bin/env python
"""
Regenerate bench/BASELINE.md by running a fixed matrix of configurations with
``--profile`` and capturing the reports.

The point is that BASELINE.md is committed, so ``git diff`` after a change
shows the performance delta directly rather than requiring you to remember
what the numbers used to be.

    python bench/run_baseline.py            # regenerate BASELINE.md
    python bench/run_baseline.py --quick    # skip the slow full-raster case

Caveats, stated in the generated file too:
  - Timings are machine-specific. A diff is meaningful when both sides were
    measured on the same machine; it is not a cross-machine comparison, and
    not a CI gate.
  - Run-to-run noise is a few percent, so small diffs mean nothing. Reports are
    embedded verbatim, so expect the last digits to churn; the changes worth
    reading are order-of-magnitude, not marginal.
  - The slow case uses --threads 1 so phase attribution is unambiguous
    (summed thread time == elapsed). A separate default-threads run captures
    what concurrency is actually achieved.
"""

from __future__ import annotations

import argparse
import os
import platform
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
OUT = REPO / "bench" / "BASELINE.md"

SMALL = "tests/data/se-island.tiff"
LARGE = "tests/data/input/TestDEM_tiled.tif"

# (label, raster, [cli args]). Kept small on purpose: this should be cheap
# enough to re-run after every change.
CASES: list[tuple[str, str, list[str], bool]] = [
    (
        "H3 --point value (small, 3-band)",
        SMALL,
        ["-r", "12", "--point", "value", "--threads", "1"],
        False,
    ),
    (
        "H3 --overlay weighted (small, 3-band)",
        SMALL,
        ["-r", "10", "--overlay", "weighted", "--threads", "1"],
        False,
    ),
    (
        "H3 --sample bilinear (small, 3-band)",
        SMALL,
        ["-r", "12", "--sample", "bilinear", "--threads", "1"],
        False,
    ),
    (
        "H3 --point value (full DEM, 1-band, single-threaded)",
        LARGE,
        ["-r", "11", "--point", "value", "--threads", "1"],
        True,
    ),
    (
        "H3 --point value (full DEM, 1-band, default threads)",
        LARGE,
        ["-r", "11", "--point", "value"],
        True,
    ),
    # Overlay and sample on the full raster, default threads: 760 windows is
    # enough for the concurrency figure to mean something (the small raster has
    # only 2, so it cannot show more than ~2x). Coarser resolutions than the
    # --point cases because both scale with cell count, not pixel count.
    (
        "H3 --overlay weighted (full DEM, 1-band, default threads)",
        LARGE,
        ["-r", "8", "--overlay", "weighted"],
        True,
    ),
    (
        "H3 --sample bilinear (full DEM, 1-band, default threads)",
        LARGE,
        ["-r", "8", "--sample", "bilinear"],
        True,
    ),
]


def run_case(label: str, raster: str, args: list[str]) -> str:
    tmp = Path(tempfile.mkdtemp(prefix="r2d-bench-"))
    try:
        cmd = [
            sys.executable,
            "-c",
            "from raster2dggs.cli import main; main()",
            "h3",
            raster,
            str(tmp / "out"),
            *args,
            "--profile",
            "--overwrite",
        ]
        proc = subprocess.run(
            cmd, cwd=REPO, capture_output=True, text=True, env={**os.environ}
        )
        if proc.returncode != 0:
            return f"FAILED (exit {proc.returncode})\n{proc.stderr[-2000:]}"
        stderr = proc.stderr
        start = stderr.find("Profile")
        if start == -1:
            return "FAILED: no profile report in output"
        report = stderr[start:].rstrip()
        return "\n".join(line.rstrip() for line in report.splitlines())
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--quick", action="store_true", help="skip slow full-raster cases")
    ns = ap.parse_args()

    import rasterio

    from raster2dggs import __version__

    header = [
        "# Profiling baseline",
        "",
        "Regenerate with `python bench/run_baseline.py`. This file is committed so",
        "that `git diff` after a change shows the performance delta directly.",
        "",
        "**Timings are machine-specific.** A diff is only meaningful when both sides",
        "were measured on the same machine; this is a local development aid, not a",
        "cross-machine comparison and not a CI gate. Run-to-run noise is a few",
        "percent, so marginal diffs are not signal — the changes worth reading here",
        "are order-of-magnitude.",
        "",
        "Indentation in the tables is nesting, not addition: inner phases are",
        "measured inside their parent. See the `--profile` section of the README.",
        "",
        "| | |",
        "|---|---|",
        f"| raster2dggs | {__version__} |",
        f"| Python | {platform.python_version()} |",
        f"| rasterio / GDAL | {rasterio.__version__} / {rasterio.__gdal_version__} |",
        f"| platform | {platform.system()} {platform.machine()} |",
        f"| CPU count | {os.cpu_count()} |",
        "",
    ]

    body = []
    for label, raster, args, slow in CASES:
        if slow and ns.quick:
            continue
        print(f"running: {label} ...", file=sys.stderr, flush=True)
        report = run_case(label, raster, args)
        body += [f"## {label}", "", "```", report, "```", ""]

    OUT.write_text("\n".join(header + body))
    print(f"wrote {OUT.relative_to(REPO)}", file=sys.stderr)


if __name__ == "__main__":
    main()
