"""
CLI smoke tests for --sample across all DGGS implementations.

Each DGGS has a different cells_in_bbox / cells_to_lonlat_arrays
implementation. These end-to-end tests verify the full pipeline produces valid
output for each one.

Setup: a 10 × 10 pixel Float32 raster covering a 1° × 1° bbox near Auckland,
NZ, filled uniformly with _PIXEL_VALUE.  Because the raster is uniform the
expected output value for every sampled cell is _PIXEL_VALUE (within float32
precision), making assertions straightforward.

Resolutions are chosen so that ~5-30 DGGS cell centres fall inside the raster
bbox, giving enough rows to exercise the pipeline without being slow.
"""

import unittest

import numpy as np
import pyarrow.parquet as pq
from classes.base import TestRunthrough
from classes.helpers import make_raster
from data.datapaths import TEST_OUTPUT_PATH

_BOUNDS = (174.0, -37.0, 175.0, -36.0)  # 1° × 1° near Auckland
_SIZE = 10
_PIXEL_VALUE = 42.0


def _make_raster(path: str) -> None:
    make_raster(path, _BOUNDS, _SIZE, pixel_value=_PIXEL_VALUE)


class _SampleSmoke(TestRunthrough):
    """
    Base class for per-DGGS --sample smoke tests.

    Subclasses set:
      dggs       — CLI subcommand name (e.g. "h3")
      resolution — integer to pass as -r
      extra      — pip extra name (for skip messages)
    """

    dggs: str = None
    resolution: int = None
    extra: str = None

    def setUp(self):
        if self.dggs is None:
            self.skipTest("Abstract base class — no DGGS configured")
        super().setUp()
        self._raster = self.make_temp_raster(_make_raster)

    def _run(self, *extra_args):
        return self.invoke_cli(
            self.dggs,
            self._raster,
            TEST_OUTPUT_PATH,
            self.resolution,
            "--sample",
            *extra_args,
        )

    def test_exit_code_zero(self):
        result = self._run()
        self.assertEqual(result.exit_code, 0, result.output)

    def test_produces_rows(self):
        result = self._run()
        self.assertEqual(result.exit_code, 0, result.output)
        table = pq.read_table(str(TEST_OUTPUT_PATH))
        self.assertGreater(len(table), 0, "--sample produced no output rows")

    def test_output_values_match_pixel(self):
        result = self._run()
        self.assertEqual(result.exit_code, 0, result.output)
        df = pq.read_table(str(TEST_OUTPUT_PATH)).to_pandas()
        vals = df["band_1"].dropna()
        self.assertGreater(len(vals), 0, "All output values were NaN")
        self.assertTrue(
            np.allclose(vals, _PIXEL_VALUE, atol=1e-3),
            f"Expected all values ≈ {_PIXEL_VALUE}, got: {vals.unique()}",
        )

    def test_agg_warning_emitted_for_sample(self):
        result = self._run("--agg", "sum")
        self.assertEqual(result.exit_code, 0, result.output)
        self.assertIn(
            "--agg",
            result.output,
            "--agg warning should be emitted when combined with --sample",
        )


class TestH3Sample(_SampleSmoke, unittest.TestCase):
    # res 5: ~252 km² cells → ~28 cells in a 1° × 1° bbox
    dggs = "h3"
    resolution = 5
    extra = "h3"


class TestRHPSample(_SampleSmoke, unittest.TestCase):
    # res 6: ~1440 km² cells → ~5 cells in a 1° × 1° bbox
    dggs = "rhp"
    resolution = 6
    extra = "rhp"


class TestGeohashSample(_SampleSmoke, unittest.TestCase):
    # precision 4: lon_step ≈ 0.35°, lat_step ≈ 0.18° → ~16 cells
    dggs = "geohash"
    resolution = 4
    extra = "geohash"


class TestMaidenheadSample(_SampleSmoke, unittest.TestCase):
    # level 3: lon_step ≈ 0.083°, lat_step ≈ 0.042° → ~288 cells
    dggs = "maidenhead"
    resolution = 3
    extra = "maidenhead"


class TestS2Sample(_SampleSmoke, unittest.TestCase):
    # level 8: ~1300 km² cells → ~5 cells in a 1° × 1° bbox
    dggs = "s2"
    resolution = 8
    extra = "s2"


class TestA5Sample(_SampleSmoke, unittest.TestCase):
    # level 8: ~649 km² cells → ~11 cells in a 1° × 1° bbox
    dggs = "a5"
    resolution = 8
    extra = "a5"


# DGGAL (family representatives)
# The cells_in_bbox implementation is shared by all DGGAL variants via
# DGGALRasterIndexer.  We test one representative from each grid family to
# catch any family-specific issues (refinement ratio, coordinate handling, etc.)


class TestISEA4RSample(_SampleSmoke, unittest.TestCase):
    # level 8: ~1300 km² cells → ~5 cells in a 1° × 1° bbox
    dggs = "isea4r"
    resolution = 8
    extra = "dggal"


class TestISEA9RSample(_SampleSmoke, unittest.TestCase):
    # level 5: 6 × 9^(5-1)?  No — ISEA9R is a 9R grid; pick a level that gives ~5-10 cells.
    # At level 5: ~5 cells based on empirical testing.
    dggs = "isea9r"
    resolution = 5
    extra = "dggal"


class TestISEA3HSample(_SampleSmoke, unittest.TestCase):
    # Hexagonal 3H; level 9 needed for cells to be small enough to land in bbox.
    dggs = "isea3h"
    resolution = 9
    extra = "dggal"


class TestISEA7HSample(_SampleSmoke, unittest.TestCase):
    # Hexagonal 7H; level 6 gives ~3-6 cells in a 1° × 1° bbox.
    dggs = "isea7h"
    resolution = 6
    extra = "dggal"


class TestHEALPixSample(_SampleSmoke, unittest.TestCase):
    # HEALPix; level 8 gives ~15 cells in a 1° × 1° bbox (from earlier testing).
    dggs = "healpix"
    resolution = 8
    extra = "dggal"


class _SampleBilinearSmoke(_SampleSmoke):
    """
    Base class for --sample --interp bilinear smoke tests.

    Inherits all _SampleSmoke tests; overrides _run to add --interp bilinear.
    Because the raster is uniform, bilinear and NN produce identical values,
    so the same value assertions apply.
    """

    def _run(self, *extra_args):
        return self.invoke_cli(
            self.dggs,
            self._raster,
            TEST_OUTPUT_PATH,
            self.resolution,
            "--sample",
            "bilinear",
            *extra_args,
        )


class TestH3SampleBilinear(_SampleBilinearSmoke, unittest.TestCase):
    # Bilinear is implemented in common.py, not per-DGGS; one representative is enough.
    dggs = "h3"
    resolution = 5
    extra = "h3"


class _SampleBicubicSmoke(_SampleSmoke):
    """
    Base class for --sample --interp bicubic smoke tests.

    Inherits all _SampleSmoke tests; overrides _run to add --interp bicubic.
    Because the raster is uniform, bicubic and NN produce identical values,
    so the same value assertions apply.
    """

    def _run(self, *extra_args):
        return self.invoke_cli(
            self.dggs,
            self._raster,
            TEST_OUTPUT_PATH,
            self.resolution,
            "--sample",
            "bicubic",
            *extra_args,
        )


class TestH3SampleBicubic(_SampleBicubicSmoke, unittest.TestCase):
    # Bicubic is implemented in common.py, not per-DGGS; one representative is enough.
    dggs = "h3"
    resolution = 5
    extra = "h3"


class _SampleLanczosSmoke(_SampleSmoke):
    """
    Base class for --sample --interp lanczos smoke tests.

    Inherits all _SampleSmoke tests; overrides _run to add --interp lanczos.
    Because the raster is uniform, Lanczos and NN produce identical values,
    so the same value assertions apply.
    """

    def _run(self, *extra_args):
        return self.invoke_cli(
            self.dggs,
            self._raster,
            TEST_OUTPUT_PATH,
            self.resolution,
            "--sample",
            "lanczos",
            *extra_args,
        )


class TestH3SampleLanczos(_SampleLanczosSmoke, unittest.TestCase):
    # Lanczos is implemented in common.py, not per-DGGS; one representative is enough.
    dggs = "h3"
    resolution = 5
    extra = "h3"
