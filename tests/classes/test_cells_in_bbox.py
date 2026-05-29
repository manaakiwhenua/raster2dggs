"""
Unit tests for cells_in_bbox and cells_to_lonlat_arrays across all DGGS
implementations.

Each DGGS has a different cells_in_bbox implementation; these tests verify the
shared contract for all of them:

  1. SUPPORTS_CELL_ENUMERATION is True
  2. The return type is a set of strings
  3. The result is non-empty for a known geographic bbox
  4. Every returned cell's centroid lies strictly inside that bbox
     (verified by round-tripping through cells_to_lonlat_arrays)

Test bbox: 1° × 1° near Auckland, NZ — a region with no antimeridian issues
and enough cells at moderate resolutions to give meaningful coverage without
being slow.
"""

import importlib

import numpy as np
import pandas as pd
import pytest

_MIN_LON, _MIN_LAT, _MAX_LON, _MAX_LAT = 174.0, -37.0, 175.0, -36.0

_DGGS_CASES = [
    # (dggs_name, resolution, module_path, class_name)
    # res chosen so ~5-30 cells fall inside the 1° × 1° bbox
    ("h3", 5, "raster2dggs.indexers.h3rasterindexer", "H3RasterIndexer"),
    ("rhp", 6, "raster2dggs.indexers.rhprasterindexer", "RHPRasterIndexer"),
    ("geohash", 4, "raster2dggs.indexers.geohashrasterindexer", "GeohashRasterIndexer"),
    (
        "maidenhead",
        3,
        "raster2dggs.indexers.maidenheadrasterindexer",
        "MaidenheadRasterIndexer",
    ),
    ("s2", 8, "raster2dggs.indexers.s2rasterindexer", "S2RasterIndexer"),
    ("a5", 8, "raster2dggs.indexers.a5rasterindexer", "A5RasterIndexer"),
    # DGGAL variants share the same cells_in_bbox implementation; one representative suffices
    ("isea4r", 8, "raster2dggs.indexers.dggalrasterindexer", "ISEA4RRasterIndexer"),
]


@pytest.fixture(params=_DGGS_CASES, ids=[c[0] for c in _DGGS_CASES])
def indexer_and_res(request):
    dggs, resolution, module_path, class_name = request.param
    try:
        mod = importlib.import_module(module_path)
        cls = getattr(mod, class_name)
        return cls(dggs), resolution
    except ImportError:
        pytest.skip(f"{dggs} extra not installed")


class TestCellsInBbox:
    def _cells(self, indexer, resolution):
        return indexer.cells_in_bbox(_MIN_LON, _MIN_LAT, _MAX_LON, _MAX_LAT, resolution)

    def test_supports_cell_enumeration_flag(self, indexer_and_res):
        indexer, _ = indexer_and_res
        assert indexer.SUPPORTS_CELL_ENUMERATION

    def test_returns_set(self, indexer_and_res):
        indexer, resolution = indexer_and_res
        assert isinstance(self._cells(indexer, resolution), set)

    def test_nonempty(self, indexer_and_res):
        indexer, resolution = indexer_and_res
        assert len(self._cells(indexer, resolution)) > 0

    def test_cell_ids_are_strings(self, indexer_and_res):
        indexer, resolution = indexer_and_res
        for c in self._cells(indexer, resolution):
            assert isinstance(c, str), f"Cell ID {c!r} should be a string"

    def test_all_centroids_inside_bbox(self, indexer_and_res):
        indexer, resolution = indexer_and_res
        cells = self._cells(indexer, resolution)
        lons, lats = indexer.cells_to_lonlat_arrays(pd.Series(sorted(cells)))
        assert np.all(
            lons >= _MIN_LON
        ), f"Centroid lon below min_lon: {lons[lons < _MIN_LON]}"
        assert np.all(
            lons <= _MAX_LON
        ), f"Centroid lon above max_lon: {lons[lons > _MAX_LON]}"
        assert np.all(
            lats >= _MIN_LAT
        ), f"Centroid lat below min_lat: {lats[lats < _MIN_LAT]}"
        assert np.all(
            lats <= _MAX_LAT
        ), f"Centroid lat above max_lat: {lats[lats > _MAX_LAT]}"
