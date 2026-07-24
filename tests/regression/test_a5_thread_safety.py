"""
Regression test for a concurrency crash in A5 --overlay processing.

Background: pya5 (raster2dggs/indexers/a5rasterindexer.py) keeps unlocked
module-level mutable state (a single-entry cache of the last resolved cell,
used to speed up dense-sample loops). raster2dggs calls into it from multiple
threads -- per-window in Stage 1, per-partition in Stage 2's dask
map_partitions -- with no serialisation. Under real multi-threaded load this
produced a reproducible crash: `cells_in_bbox` -> `a5.polygon_to_cells` ->
`cell_to_spherical` -> `equal_area.inverse` -> `math.acos(...)` raising
`ValueError: math domain error`, from mismatched/torn state read across
threads. Confirmed via the actual reported repro command: consistently
crashed multi-threaded, never crashed single-threaded (`--threads 1`).

Reproducing the exact race reliably in a fast test is impractical (it is
timing-dependent, and synthetic concurrent-call stress tests did not trigger
it in isolation during investigation, even though the real pipeline did).
Instead, this test verifies the actual fix mechanism directly: every
A5RasterIndexer method that calls into a5py acquires raster2dggs's own
`_A5_LOCK` first, so pya5 never sees concurrent entry regardless of how many
threads raster2dggs itself uses.
"""

import threading

import pandas as pd
import pytest

try:
    import a5

    import raster2dggs.indexers.a5rasterindexer as a5mod
except ImportError:
    pytest.skip("a5 extra not installed", allow_module_level=True)


def _lock_held_by_caller() -> bool:
    """True iff _A5_LOCK is currently held by the calling thread (checked by
    trying to acquire it, without blocking, from a *different* thread)."""
    result = {}

    def checker():
        acquired = a5mod._A5_LOCK.acquire(blocking=False)
        result["acquired"] = acquired
        if acquired:
            a5mod._A5_LOCK.release()

    t = threading.Thread(target=checker)
    t.start()
    t.join()
    return not result["acquired"]


@pytest.fixture
def indexer():
    return a5mod.A5RasterIndexer("a5")


@pytest.fixture
def assert_locked_during(monkeypatch):
    """Patches the given a5py attribute so that, while it runs, _A5_LOCK must
    already be held by the calling thread."""
    checked = []

    def _apply(attr_name):
        original = getattr(a5mod.a5py, attr_name)

        def wrapper(*args, **kwargs):
            checked.append(attr_name)
            assert (
                _lock_held_by_caller()
            ), f"a5py.{attr_name} was called without _A5_LOCK held"
            return original(*args, **kwargs)

        monkeypatch.setattr(a5mod.a5py, attr_name, wrapper)

    yield _apply
    assert checked, "patched a5py attribute was never actually called"


def test_cells_in_bbox_holds_lock(indexer, assert_locked_during):
    assert_locked_during("polygon_to_cells")
    indexer.cells_in_bbox(170.0, -41.0, 171.0, -40.0, 6)


def test_index_window_holds_lock(indexer, assert_locked_during):
    assert_locked_during("lonlat_to_cell")
    wide = pd.DataFrame({"x": [170.5, 170.6], "y": [-40.5, -40.6]})
    indexer._index_window(wide, resolution=6, parent_res=2)


def test_cell_area_m2_holds_lock(indexer, assert_locked_during):
    assert_locked_during("cell_area")
    indexer.cell_area_m2(6, -40.5, 170.5)


def test_valid_set_holds_lock(indexer, assert_locked_during):
    assert_locked_during("get_resolution")
    cell = a5.u64_to_hex(a5.lonlat_to_cell((170.5, -40.5), 6))
    indexer.valid_set({cell})


def test_parent_cells_holds_lock(indexer, assert_locked_during):
    assert_locked_during("cell_to_parent")
    cell = a5.u64_to_hex(a5.lonlat_to_cell((170.5, -40.5), 6))
    list(indexer.parent_cells({cell}, 2))


def test_expected_count_holds_lock(indexer, assert_locked_during):
    assert_locked_during("hex_to_u64")
    cell = a5.u64_to_hex(a5.lonlat_to_cell((170.5, -40.5), 6))
    indexer.expected_count(cell, 8)


def test_cells_to_lonlat_arrays_holds_lock(indexer, assert_locked_during):
    assert_locked_during("cell_to_lonlat")
    cell = a5.u64_to_hex(a5.lonlat_to_cell((170.5, -40.5), 6))
    indexer.cells_to_lonlat_arrays(pd.Series([cell]))


def test_cell_to_point_holds_lock(indexer, assert_locked_during):
    assert_locked_during("cell_to_lonlat")
    cell = a5.u64_to_hex(a5.lonlat_to_cell((170.5, -40.5), 6))
    indexer.cell_to_point(cell)


def test_cell_to_polygon_holds_lock(indexer, assert_locked_during):
    assert_locked_during("cell_to_boundary")
    cell = a5.u64_to_hex(a5.lonlat_to_cell((170.5, -40.5), 6))
    indexer.cell_to_polygon(cell)
