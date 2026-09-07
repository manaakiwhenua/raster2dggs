"""
Unit tests for the rHEALPix indexer: the "" no-cell sentinel from
cells_from_points must be filtered like any other missing cell, and
suid-truncation parents must agree with rhp_wrappers.rhp_to_parent.
"""

import numpy as np
import pandas as pd
import pytest

try:
    import rhealpixdggs.rhp_wrappers as rhpw

    import raster2dggs.indexers.rhprasterindexer as rhpmod
except ImportError:
    pytest.skip("rhp extra not installed", allow_module_level=True)


@pytest.fixture
def indexer():
    return rhpmod.RHPRasterIndexer("rhp")


def test_index_window_maps_empty_sentinel_to_na(indexer, monkeypatch):
    # cells_from_points returns "" for a point that no cell contains.
    monkeypatch.setattr(
        rhpmod.WGS84_003,
        "cells_from_points",
        lambda u, v, res, plane: np.array(["", "Q333333"], dtype=object),
    )
    wide = pd.DataFrame({"x": [0.0, 170.5], "y": [0.0, -40.5], "band_1": [1.0, 2.0]})
    out = indexer._index_window(wide, resolution=6, parent_res=2)
    idx = out[indexer.index_col(6)]
    parent = out[indexer.partition_col(2)]
    assert pd.isna(idx.iloc[0]) and pd.isna(parent.iloc[0])
    assert idx.iloc[1] == "Q333333" and parent.iloc[1] == "Q33"
    assert "x" not in out.columns and "y" not in out.columns


def test_index_window_agrees_with_scalar_wrapper(indexer):
    rng = np.random.default_rng(7)
    lons = rng.uniform(165, 180, 50)
    lats = rng.uniform(-47, -34, 50)
    wide = pd.DataFrame({"x": lons, "y": lats, "band_1": np.arange(50.0)})
    out = indexer._index_window(wide, resolution=8, parent_res=3)
    expected = [
        rhpw.geo_to_rhp(la, lo, 8, plane=False)
        for la, lo in zip(lats, lons, strict=True)
    ]
    assert list(out[indexer.index_col(8)]) == expected
    assert list(out[indexer.partition_col(3)]) == [c[:4] for c in expected]


def test_valid_set_filters_na_and_empty(indexer):
    assert indexer.valid_set({"Q333", "", None, float("nan"), "N1"}) == {"Q333", "N1"}


@pytest.mark.parametrize("cell", ["N", "Q3", "Q333", "S001450634"])
def test_parent_cells_matches_rhp_to_parent(indexer, cell):
    for res in range(0, len(cell) + 2):
        assert indexer.parent_cells({cell}, res) == [rhpw.rhp_to_parent(cell, res)]


def test_base_cell_is_its_own_parent(indexer):
    assert indexer.parent_cells({"N"}, 0) == ["N"]
    # Requesting a resolution at or below the cell's own returns it unchanged.
    assert indexer.parent_cells({"Q33"}, 5) == ["Q33"]


def test_batch_geometry_matches_scalar(indexer):
    cells = ["N1", "Q333", "S00145", "R87"]
    batch_polys = indexer.cells_to_polygons(cells)
    batch_pts = indexer.cells_to_points(cells)
    for c, poly, pt in zip(cells, batch_polys, batch_pts, strict=True):
        assert poly.equals_exact(indexer.cell_to_polygon(c), 1e-12)
        assert pt.equals_exact(indexer.cell_to_point(c), 1e-9)
