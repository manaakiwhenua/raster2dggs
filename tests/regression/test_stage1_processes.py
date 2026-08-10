"""
Stage 1's worker pool: that it is real, and that it changes nothing.

The whole point of the pool is that each worker has its own interpreter and so
its own GIL. Nothing else in the suite would notice if it silently degraded to
running everything in the parent -- every output would still be correct, only
slower -- so the distinct-PID assertion here is load-bearing.
"""

import multiprocessing
import os
from concurrent.futures import ProcessPoolExecutor

import pytest
import rasterio
from classes.helpers import make_raster
from click.testing import CliRunner

from raster2dggs import common
from raster2dggs.cli import cli
from raster2dggs.profiling import PROFILER, Profiler

_BOUNDS = (174.0, -41.2, 174.2, -41.0)
_SIZE = 48
_RES = 9


@pytest.fixture(scope="module")
def raster(tmp_path_factory):
    """A raster with several windows, so a pool has something to distribute.

    make_raster writes an untiled GeoTIFF, which is strip-encoded and therefore
    reports one window per row -- enough windows without needing to tile.
    """
    path = tmp_path_factory.mktemp("proc") / "multi_window.tif"
    make_raster(str(path), _BOUNDS, _SIZE, pixel_value=7.0)
    with rasterio.open(path) as src:
        assert len(list(src.block_windows())) > 1, "fixture must have several windows"
    return str(path)


def _invoke(raster, out, *extra):
    out.mkdir(parents=True, exist_ok=True)
    result = CliRunner().invoke(
        cli,
        ["h3", raster, str(out), "-r", str(_RES), "--overwrite", *extra],
        catch_exceptions=False,
    )
    assert result.exit_code == 0, result.output
    return result


def _cfg(raster, tmpdir, **over):
    cfg = {
        "raster_input": raster,
        "dggs": "h3",
        "resolution": _RES,
        "parent_res": 3,
        "selected_indices": (1,),
        "selected_labels": ("band_1",),
        "nodata_policy": "omit",
        "emit_nodata_value": None,
        "transfer": "point",
        "interp": "nn",
        "op": None,
        "out": None,
        "min_valid_coverage": 0.0,
        "decimals": None,
        "hist_spec": None,
        "compression": "snappy",
        "tmpdir": str(tmpdir),
        "profile": True,
        "gdal_cachemax_mb": 64,
        "overlay_shared_paths": {},
    }
    cfg.update(over)
    return cfg


def test_pool_runs_windows_in_other_processes(raster, tmp_path):
    """Windows must execute somewhere other than the parent process.

    Every output in the suite would still be correct if the pool silently ran
    everything in the parent, so this is the only test that would notice.
    """
    with rasterio.open(raster) as src:
        windows = [
            (w.col_off, w.row_off, w.width, w.height) for _, w in src.block_windows()
        ]

    cfg = _cfg(raster, tmp_path)
    with ProcessPoolExecutor(
        max_workers=3,
        initializer=common._init_stage1_worker,
        initargs=(cfg,),
        mp_context=multiprocessing.get_context(common._START_METHOD),
    ) as ex:
        pids = {result[0] for result in ex.map(common._run_stage1_window, windows)}

    assert pids, "no windows reported back"
    assert os.getpid() not in pids, "windows ran in the parent process"
    # Deliberately not asserting that more than one worker participated: the
    # windows here are one raster row each, so a single worker can finish them
    # all before the others are ready. Work distribution is the executor's
    # business; what matters here is that the work left this process.


def test_inline_runs_windows_in_this_process(raster, tmp_path):
    """The converse, so the test above cannot pass by doing nothing at all."""
    with rasterio.open(raster) as src:
        window = next(
            (w.col_off, w.row_off, w.width, w.height) for _, w in src.block_windows()
        )

    PROFILER.reset(enabled=True)
    common._setup_stage1_worker(_cfg(raster, tmp_path))
    try:
        pid, _snapshot = common._run_stage1_window(window)
    finally:
        common._close_stage1_worker()
        PROFILER.reset(enabled=False)

    assert pid == os.getpid()


def test_profile_aggregates_worker_measurements(raster, tmp_path):
    """A worker's phase timings have to travel back, or the report is empty of
    everything except what the parent itself did."""
    result = _invoke(raster, tmp_path / "prof", "--processes", "4", "--profile")

    assert "Profile" in result.output
    # window_total is only ever entered inside a worker.
    assert "window_total" in result.output
    assert "dggs_index" in result.output
    assert "Stage 1 parallelism" in result.output
    # Counters accumulate in the workers too.
    assert "pixels_read" in result.output


def test_worker_snapshots_are_summed_not_replaced():
    """merge() adds, so N workers each reporting a window give N windows."""
    parent = Profiler()
    parent.reset(enabled=True)
    for _ in range(3):
        worker = Profiler()
        worker.reset(enabled=True)
        with worker.phase("stage1.window_total"):
            pass
        worker.add("rows_indexed", 10)
        parent.merge(worker.snapshot())

    snap = parent.snapshot()
    assert snap["counts"]["stage1.window_total"] == 3
    assert snap["counters"]["rows_indexed"] == 30


def test_profiling_disabled_sends_nothing_back(raster, tmp_path):
    """The per-window return value is only paid for when --profile is on."""
    PROFILER.reset(enabled=False)
    common._setup_stage1_worker(_cfg(raster, tmp_path, profile=False))
    try:
        with rasterio.open(raster) as src:
            w = next(w for _, w in src.block_windows())
        assert (
            common._run_stage1_window((w.col_off, w.row_off, w.width, w.height)) is None
        )
    finally:
        common._close_stage1_worker()
