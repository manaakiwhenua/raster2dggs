"""
Lock discipline for the rHEALPix indexer.

rhealpixdggs's WGS84_003 singleton lazily populates RHEALPixDGGS._projection_cache
via an unlocked check-then-write, so it is not safe under concurrent access.
raster2dggs calls into it from multiple threads (per-window in Stage 1,
per-partition in Stage 2's dask map_partitions). As with the a5 indexer
(tests/regression/test_a5_thread_safety.py), reproducing the race directly is
impractical; instead this verifies the fix mechanism: every RHPRasterIndexer
method that calls into rhealpixdggs acquires raster2dggs's own _RHP_LOCK first.
Methods that are pure suid string arithmetic (parent_cells, expected_count,
cell_to_children_size, valid_set, cell_area_m2) intentionally take no lock and
are not tested here.
"""

import threading

import pandas as pd
import pytest

try:
    import raster2dggs.indexers.rhprasterindexer as rhpmod
except ImportError:
    pytest.skip("rhp extra not installed", allow_module_level=True)


def _lock_held_by_caller() -> bool:
    """True iff _RHP_LOCK is currently held by the calling thread (checked by
    trying to acquire it, without blocking, from a *different* thread)."""
    result = {}

    def checker():
        acquired = rhpmod._RHP_LOCK.acquire(blocking=False)
        result["acquired"] = acquired
        if acquired:
            rhpmod._RHP_LOCK.release()

    t = threading.Thread(target=checker)
    t.start()
    t.join()
    return not result["acquired"]


@pytest.fixture
def indexer():
    return rhpmod.RHPRasterIndexer("rhp")


@pytest.fixture
def assert_locked_during(monkeypatch):
    """Patches the given attribute on the target object so that, while it
    runs, _RHP_LOCK must already be held by the calling thread."""
    checked = []

    def _apply(target, attr_name):
        original = getattr(target, attr_name)

        def wrapper(*args, **kwargs):
            checked.append(attr_name)
            assert (
                _lock_held_by_caller()
            ), f"{attr_name} was called without _RHP_LOCK held"
            return original(*args, **kwargs)

        monkeypatch.setattr(target, attr_name, wrapper)

    yield _apply
    assert checked, "patched rhealpixdggs attribute was never actually called"


def test_index_window_holds_lock(indexer, assert_locked_during):
    assert_locked_during(rhpmod.WGS84_003, "cells_from_points")
    wide = pd.DataFrame({"x": [170.5, 170.6], "y": [-40.5, -40.6]})
    indexer._index_window(wide, resolution=6, parent_res=2)


def test_cells_in_bbox_holds_lock(indexer, assert_locked_during):
    assert_locked_during(rhpmod.rhpw, "polyfill_array")
    indexer.cells_in_bbox(170.0, -41.0, 171.0, -40.0, 5)


def test_cells_to_lonlat_arrays_holds_lock(indexer, assert_locked_during):
    assert_locked_during(rhpmod.WGS84_003, "centroids")
    indexer.cells_to_lonlat_arrays(pd.Series(["Q333"]))


def test_cell_to_point_holds_lock(indexer, assert_locked_during):
    assert_locked_during(rhpmod.rhpw, "rhp_to_geo")
    indexer.cell_to_point("Q333")


def test_cell_to_polygon_holds_lock(indexer, assert_locked_during):
    assert_locked_during(rhpmod.rhpw, "rhp_to_geo_boundary")
    indexer.cell_to_polygon("Q333")


def test_cells_to_points_holds_lock(indexer, assert_locked_during):
    assert_locked_during(rhpmod.WGS84_003, "centroids")
    indexer.cells_to_points(["Q333"])


def test_cells_to_polygons_holds_lock(indexer, assert_locked_during):
    assert_locked_during(rhpmod.WGS84_003, "boundary_array")
    indexer.cells_to_polygons(["Q333"])


def test_cells_overlapping_bbox_holds_lock(indexer, assert_locked_during):
    assert_locked_during(rhpmod.rhpw, "polyfill_array")
    indexer.cells_overlapping_bbox(170.0, -41.0, 171.0, -40.0, 5)
