import tempfile
from pathlib import Path
from unittest import TestCase

import numpy as np
import rasterio
from rasterio.crs import CRS
from rasterio.transform import from_bounds
from click.testing import CliRunner

from classes.base import TestRunthrough
from data.datapaths import TEST_OUTPUT_PATH
from raster2dggs.cli import cli
from raster2dggs.constants import ResolutionMode
import raster2dggs.common as common
from raster2dggs.indexers.h3rasterindexer import H3RasterIndexer

# Small single-band WGS84 raster — pixel size ≈ 0.01° × 0.01° near Auckland
_BOUNDS = (174.0, -41.1, 174.1, -41.0)  # (left, bottom, right, top)
_SIZE = 10  # 10 × 10 pixels
_H3_MIN, _H3_MAX = 0, 15


def _make_raster(path: str) -> None:
    data = np.ones((1, _SIZE, _SIZE), dtype=np.float32)
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
        nodata=None,
    ) as dst:
        dst.write(data)


class TestCellAreaM2(TestCase):
    """cell_area_m2 returns positive, monotonically decreasing values for H3."""

    def setUp(self):
        self.indexer = H3RasterIndexer("h3")
        self.lat, self.lon = -41.05, 174.05

    def test_positive_at_all_resolutions(self):
        for res in range(_H3_MIN, _H3_MAX + 1):
            area = self.indexer.cell_area_m2(res, self.lat, self.lon)
            self.assertGreater(area, 0, f"H3 res {res} area must be positive")

    def test_decreases_with_resolution(self):
        areas = [
            self.indexer.cell_area_m2(res, self.lat, self.lon)
            for res in range(_H3_MIN, _H3_MAX + 1)
        ]
        for i in range(len(areas) - 1):
            self.assertGreater(
                areas[i],
                areas[i + 1],
                f"H3 area should decrease from res {i} to res {i + 1}",
            )

    def test_coarsest_larger_than_finest(self):
        coarsest = self.indexer.cell_area_m2(_H3_MIN, self.lat, self.lon)
        finest = self.indexer.cell_area_m2(_H3_MAX, self.lat, self.lon)
        self.assertGreater(coarsest, finest)


class TestComputePixelAreaM2(TestCase):
    """compute_pixel_area_m2 returns a positive area and plausible centre."""

    def setUp(self):
        self._tmp = tempfile.NamedTemporaryFile(suffix=".tiff", delete=False)
        _make_raster(self._tmp.name)
        self.warp_args = common.assemble_warp_args("average", 12000)

    def tearDown(self):
        Path(self._tmp.name).unlink(missing_ok=True)

    def test_returns_positive_area(self):
        area, _, _ = common.compute_pixel_area_m2(self._tmp.name, self.warp_args)
        self.assertGreater(area, 0)

    def test_centre_within_bounds(self):
        left, bottom, right, top = _BOUNDS
        _, clat, clon = common.compute_pixel_area_m2(self._tmp.name, self.warp_args)
        self.assertGreaterEqual(clat, bottom)
        self.assertLessEqual(clat, top)
        self.assertGreaterEqual(clon, left)
        self.assertLessEqual(clon, right)

    def test_area_plausible_for_pixel_size(self):
        # 0.01° × 0.01° at ~41°S → roughly 600 000 – 1 200 000 m²
        area, _, _ = common.compute_pixel_area_m2(self._tmp.name, self.warp_args)
        self.assertGreater(area, 5e5)
        self.assertLess(area, 2e6)


class TestResolveModeInvariants(TestCase):
    """
    Each mode must satisfy its defining property against the pixel area of the
    test raster.  We check invariants rather than hard-coded resolutions so the
    tests don't break if Earth-model constants are refined.
    """

    def setUp(self):
        self._tmp = tempfile.NamedTemporaryFile(suffix=".tiff", delete=False)
        _make_raster(self._tmp.name)
        self.warp_args = common.assemble_warp_args("average", 12000)
        self.indexer = H3RasterIndexer("h3")
        self.pixel_area, self.clat, self.clon = common.compute_pixel_area_m2(
            self._tmp.name, self.warp_args
        )

    def tearDown(self):
        Path(self._tmp.name).unlink(missing_ok=True)

    def _resolve(self, mode):
        return common.resolve_resolution_mode(
            mode, "h3", self._tmp.name, self.warp_args, _H3_MIN, _H3_MAX
        )

    def _cell_area(self, res):
        return self.indexer.cell_area_m2(res, self.clat, self.clon)

    def test_smaller_than_pixel_cell_is_smaller(self):
        res = self._resolve("smaller-than-pixel")
        self.assertLessEqual(
            self._cell_area(res),
            self.pixel_area,
            "smaller-than-pixel: selected cell must be <= pixel area",
        )

    def test_smaller_than_pixel_predecessor_is_larger(self):
        res = self._resolve("smaller-than-pixel")
        if res > _H3_MIN:
            self.assertGreater(
                self._cell_area(res - 1),
                self.pixel_area,
                "smaller-than-pixel: cell at res-1 must be > pixel area",
            )

    def test_larger_than_pixel_cell_is_larger(self):
        res = self._resolve("larger-than-pixel")
        self.assertGreaterEqual(
            self._cell_area(res),
            self.pixel_area,
            "larger-than-pixel: selected cell must be >= pixel area",
        )

    def test_larger_than_pixel_successor_is_smaller(self):
        res = self._resolve("larger-than-pixel")
        if res < _H3_MAX:
            self.assertLess(
                self._cell_area(res + 1),
                self.pixel_area,
                "larger-than-pixel: cell at res+1 must be < pixel area",
            )

    def test_larger_than_pixel_is_coarser_than_or_equal_to_smaller_than_pixel(self):
        res_smaller = self._resolve("smaller-than-pixel")
        res_larger = self._resolve("larger-than-pixel")
        self.assertLessEqual(
            res_larger,
            res_smaller,
            "larger-than-pixel resolution must be <= smaller-than-pixel resolution",
        )

    def test_min_diff_minimises_area_difference(self):
        res = self._resolve("min-diff")
        best_diff = abs(self._cell_area(res) - self.pixel_area)
        for other_res in range(_H3_MIN, _H3_MAX + 1):
            if other_res == res:
                continue
            self.assertLessEqual(
                best_diff,
                abs(self._cell_area(other_res) - self.pixel_area),
                f"min-diff res {res} is not the closest to pixel area "
                f"(res {other_res} is closer)",
            )

    def test_result_within_valid_range(self):
        for mode in ResolutionMode:
            with self.subTest(mode=mode):
                res = self._resolve(mode)
                self.assertGreaterEqual(res, _H3_MIN)
                self.assertLessEqual(res, _H3_MAX)


class TestResolutionModeCLI(TestRunthrough):
    """CLI accepts all three mode strings and produces output successfully."""

    def setUp(self):
        super().setUp()
        self._tmp = tempfile.NamedTemporaryFile(suffix=".tiff", delete=False)
        _make_raster(self._tmp.name)

    def tearDown(self):
        super().tearDown()
        Path(self._tmp.name).unlink(missing_ok=True)

    def _run(self, mode):
        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["h3", self._tmp.name, str(TEST_OUTPUT_PATH), "-r", mode],
            catch_exceptions=False,
        )
        return result

    def test_all_modes_exit_zero(self):
        for mode in ResolutionMode:
            if TEST_OUTPUT_PATH.exists():
                self.clearOutFolder(TEST_OUTPUT_PATH)
            TEST_OUTPUT_PATH.mkdir(exist_ok=True)
            with self.subTest(mode=mode):
                result = self._run(mode)
                self.assertEqual(
                    result.exit_code,
                    0,
                    f"mode '{mode}' failed:\n{result.output}",
                )

    def test_invalid_mode_string_exits_nonzero(self):
        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["h3", self._tmp.name, str(TEST_OUTPUT_PATH), "-r", "not-a-mode"],
        )
        self.assertNotEqual(result.exit_code, 0)

    def test_invalid_integer_out_of_range_exits_nonzero(self):
        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["h3", self._tmp.name, str(TEST_OUTPUT_PATH), "-r", "99"],
        )
        self.assertNotEqual(result.exit_code, 0)
