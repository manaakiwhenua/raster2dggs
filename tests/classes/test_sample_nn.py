"""
CLI smoke tests for --transfer sample_nn across all DGGS implementations.

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

import tempfile
import unittest
from pathlib import Path

import numpy as np
import pyarrow.parquet as pq
import rasterio
from rasterio.crs import CRS
from rasterio.transform import from_bounds
from click.testing import CliRunner

from classes.base import TestRunthrough
from data.datapaths import TEST_OUTPUT_PATH
from raster2dggs.cli import cli

_BOUNDS = (174.0, -37.0, 175.0, -36.0)  # 1° × 1° near Auckland
_SIZE = 10
_PIXEL_VALUE = 42.0


def _make_raster(path: str) -> None:
    data = np.full((1, _SIZE, _SIZE), _PIXEL_VALUE, dtype=np.float32)
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        height=_SIZE,
        width=_SIZE,
        count=1,
        dtype="float32",
        crs=CRS.from_epsg(4326),
        transform=from_bounds(*_BOUNDS, _SIZE, _SIZE),
    ) as dst:
        dst.write(data)


class _SampleNNSmoke(TestRunthrough):
    """
    Base class for per-DGGS sample_nn smoke tests.

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
        self._raster = tempfile.NamedTemporaryFile(suffix=".tiff", delete=False)
        _make_raster(self._raster.name)

    def tearDown(self):
        Path(self._raster.name).unlink(missing_ok=True)
        super().tearDown()

    def _run(self, *extra_args):
        if TEST_OUTPUT_PATH.exists():
            self.clearOutFolder(TEST_OUTPUT_PATH)
        TEST_OUTPUT_PATH.mkdir(exist_ok=True)
        runner = CliRunner()
        return runner.invoke(
            cli,
            [
                self.dggs,
                self._raster.name,
                str(TEST_OUTPUT_PATH),
                "-r",
                str(self.resolution),
                "--semantics",
                "point_sample_field",
                "--transfer",
                "sample_nn",
                "--out",
                "value",
            ]
            + list(extra_args),
            catch_exceptions=False,
        )

    def test_exit_code_zero(self):
        result = self._run()
        self.assertEqual(result.exit_code, 0, result.output)

    def test_produces_rows(self):
        result = self._run()
        self.assertEqual(result.exit_code, 0, result.output)
        table = pq.read_table(str(TEST_OUTPUT_PATH))
        self.assertGreater(len(table), 0, "sample_nn produced no output rows")

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

    def test_agg_warning_emitted_for_sample_nn(self):
        result = self._run("--agg", "sum")
        self.assertEqual(result.exit_code, 0, result.output)
        self.assertIn(
            "--agg",
            result.output,
            "--agg warning should be emitted when combined with --transfer sample_nn",
        )


class TestH3SampleNN(_SampleNNSmoke, unittest.TestCase):
    # res 5: ~252 km² cells → ~28 cells in a 1° × 1° bbox
    dggs = "h3"
    resolution = 5
    extra = "h3"


class TestRHPSampleNN(_SampleNNSmoke, unittest.TestCase):
    # res 6: ~1440 km² cells → ~5 cells in a 1° × 1° bbox
    dggs = "rhp"
    resolution = 6
    extra = "rhp"


class TestGeohashSampleNN(_SampleNNSmoke, unittest.TestCase):
    # precision 4: lon_step ≈ 0.35°, lat_step ≈ 0.18° → ~16 cells
    dggs = "geohash"
    resolution = 4
    extra = "geohash"


class TestMaidenheadSampleNN(_SampleNNSmoke, unittest.TestCase):
    # level 3: lon_step ≈ 0.083°, lat_step ≈ 0.042° → ~288 cells
    dggs = "maidenhead"
    resolution = 3
    extra = "maidenhead"


class TestS2SampleNN(_SampleNNSmoke, unittest.TestCase):
    # level 8: ~1300 km² cells → ~5 cells in a 1° × 1° bbox
    dggs = "s2"
    resolution = 8
    extra = "s2"


class TestA5SampleNN(_SampleNNSmoke, unittest.TestCase):
    # level 8: ~649 km² cells → ~11 cells in a 1° × 1° bbox
    dggs = "a5"
    resolution = 8
    extra = "a5"


# DGGAL (family representatives)
# The cells_in_bbox implementation is shared by all DGGAL variants via
# DGGALRasterIndexer.  We test one representative from each grid family to
# catch any family-specific issues (refinement ratio, coordinate handling, etc.)


class TestISEA4RSampleNN(_SampleNNSmoke, unittest.TestCase):
    # level 8: ~1300 km² cells → ~5 cells in a 1° × 1° bbox
    dggs = "isea4r"
    resolution = 8
    extra = "dggal"


class TestISEA9RSampleNN(_SampleNNSmoke, unittest.TestCase):
    # level 5: 6 × 9^(5-1)?  No — ISEA9R is a 9R grid; pick a level that gives ~5-10 cells.
    # At level 5: ~5 cells based on empirical testing.
    dggs = "isea9r"
    resolution = 5
    extra = "dggal"


class TestISEA3HSampleNN(_SampleNNSmoke, unittest.TestCase):
    # Hexagonal 3H; level 9 needed for cells to be small enough to land in bbox.
    dggs = "isea3h"
    resolution = 9
    extra = "dggal"


class TestISEA7HSampleNN(_SampleNNSmoke, unittest.TestCase):
    # Hexagonal 7H; level 6 gives ~3-6 cells in a 1° × 1° bbox.
    dggs = "isea7h"
    resolution = 6
    extra = "dggal"


class TestHEALPixSampleNN(_SampleNNSmoke, unittest.TestCase):
    # HEALPix; level 8 gives ~15 cells in a 1° × 1° bbox (from earlier testing).
    dggs = "healpix"
    resolution = 8
    extra = "dggal"
