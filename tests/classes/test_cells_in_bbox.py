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

import unittest

import numpy as np
import pandas as pd

_MIN_LON, _MIN_LAT, _MAX_LON, _MAX_LAT = 174.0, -37.0, 175.0, -36.0


class _CellsInBboxTests:
    """
    Mixin providing the standard cells_in_bbox test suite.

    Subclasses must set:
      cls.indexer    — an instantiated RasterIndexer
      cls.resolution — integer resolution to test at
    or call cls.skipTest() from setUpClass if the dependency is absent.
    """

    indexer = None
    resolution = None

    def _cells(self):
        return self.indexer.cells_in_bbox(
            _MIN_LON, _MIN_LAT, _MAX_LON, _MAX_LAT, self.resolution
        )

    def test_supports_cell_enumeration_flag(self):
        self.assertTrue(self.indexer.SUPPORTS_CELL_ENUMERATION)

    def test_returns_set(self):
        self.assertIsInstance(self._cells(), set)

    def test_nonempty(self):
        self.assertGreater(len(self._cells()), 0)

    def test_cell_ids_are_strings(self):
        for c in self._cells():
            self.assertIsInstance(c, str, f"Cell ID {c!r} should be a string")

    def test_all_centroids_inside_bbox(self):
        cells = self._cells()
        lons, lats = self.indexer.cells_to_lonlat_arrays(pd.Series(sorted(cells)))
        self.assertTrue(
            np.all(lons >= _MIN_LON),
            f"Centroid lon below min_lon: {lons[lons < _MIN_LON]}",
        )
        self.assertTrue(
            np.all(lons <= _MAX_LON),
            f"Centroid lon above max_lon: {lons[lons > _MAX_LON]}",
        )
        self.assertTrue(
            np.all(lats >= _MIN_LAT),
            f"Centroid lat below min_lat: {lats[lats < _MIN_LAT]}",
        )
        self.assertTrue(
            np.all(lats <= _MAX_LAT),
            f"Centroid lat above max_lat: {lats[lats > _MAX_LAT]}",
        )


class TestH3CellsInBbox(_CellsInBboxTests, unittest.TestCase):
    # res 5: ~252 km² cells → ~28 cells in a 1° × 1° bbox at -37° lat
    resolution = 5

    @classmethod
    def setUpClass(cls):
        try:
            from raster2dggs.indexers.h3rasterindexer import H3RasterIndexer

            cls.indexer = H3RasterIndexer("h3")
        except ImportError:
            cls.indexer = None

    def setUp(self):
        if self.indexer is None:
            self.skipTest("h3 extra not installed")


class TestRHPCellsInBbox(_CellsInBboxTests, unittest.TestCase):
    # res 6: ~1440 km² cells → ~5 cells in a 1° × 1° bbox
    resolution = 6

    @classmethod
    def setUpClass(cls):
        try:
            from raster2dggs.indexers.rhprasterindexer import RHPRasterIndexer

            cls.indexer = RHPRasterIndexer("rhp")
        except ImportError:
            cls.indexer = None

    def setUp(self):
        if self.indexer is None:
            self.skipTest("rhp extra not installed")


class TestGeohashCellsInBbox(_CellsInBboxTests, unittest.TestCase):
    # precision 4: lon_step ≈ 0.35°, lat_step ≈ 0.18° → ~16 cells in 1° × 1°
    resolution = 4

    @classmethod
    def setUpClass(cls):
        try:
            from raster2dggs.indexers.geohashrasterindexer import GeohashRasterIndexer

            cls.indexer = GeohashRasterIndexer("geohash")
        except ImportError:
            cls.indexer = None

    def setUp(self):
        if self.indexer is None:
            self.skipTest("geohash extra not installed")


class TestMaidenheadCellsInBbox(_CellsInBboxTests, unittest.TestCase):
    # level 3: lon_step ≈ 0.083°, lat_step ≈ 0.042° → ~288 cells in 1° × 1°
    resolution = 3

    @classmethod
    def setUpClass(cls):
        try:
            from raster2dggs.indexers.maidenheadrasterindexer import (
                MaidenheadRasterIndexer,
            )

            cls.indexer = MaidenheadRasterIndexer("maidenhead")
        except ImportError:
            cls.indexer = None

    def setUp(self):
        if self.indexer is None:
            self.skipTest("maidenhead extra not installed")


class TestS2CellsInBbox(_CellsInBboxTests, unittest.TestCase):
    # level 8: 6 × 4^8 = 393216 cells globally → ~1300 km² each → ~5 cells
    resolution = 8

    @classmethod
    def setUpClass(cls):
        try:
            from raster2dggs.indexers.s2rasterindexer import S2RasterIndexer

            cls.indexer = S2RasterIndexer("s2")
        except ImportError:
            cls.indexer = None

    def setUp(self):
        if self.indexer is None:
            self.skipTest("s2 extra not installed")


class TestA5CellsInBbox(_CellsInBboxTests, unittest.TestCase):
    # level 8: 12 × 4^8 = 786432 cells globally → ~649 km² each → ~11 cells
    resolution = 8

    @classmethod
    def setUpClass(cls):
        try:
            from raster2dggs.indexers.a5rasterindexer import A5RasterIndexer

            cls.indexer = A5RasterIndexer("a5")
        except ImportError:
            cls.indexer = None

    def setUp(self):
        if self.indexer is None:
            self.skipTest("a5 extra not installed")


# DGGAL (ISEA4R as representative)
# All DGGAL variants share the same cells_in_bbox implementation in
# DGGALRasterIndexer via listZones(), so testing one is sufficient here.


class TestISEA4RCellsInBbox(_CellsInBboxTests, unittest.TestCase):
    # level 8: similar density to A5/S2 level 8 → ~10-15 cells
    resolution = 8

    @classmethod
    def setUpClass(cls):
        try:
            from raster2dggs.indexers.dggalrasterindexer import ISEA4RRasterIndexer

            cls.indexer = ISEA4RRasterIndexer("isea4r")
        except ImportError:
            cls.indexer = None

    def setUp(self):
        if self.indexer is None:
            self.skipTest("dggal extra not installed")
