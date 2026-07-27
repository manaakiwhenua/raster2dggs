import importlib

import pytest
from click.testing import CliRunner

from classes.base import TestRunthrough
from classes.helpers import make_raster
from data.datapaths import TEST_OUTPUT_PATH
from raster2dggs.cli import cli
from raster2dggs.cli_factory import SPECS
from raster2dggs.constants import ResolutionMode
from raster2dggs.indexerfactory import INDEXER_LOOKUP
import raster2dggs.common as common
from raster2dggs.indexers.h3rasterindexer import H3RasterIndexer

# Small single-band WGS84 raster — pixel size ≈ 0.01° × 0.01° near Auckland
_BOUNDS = (174.0, -41.1, 174.1, -41.0)  # (left, bottom, right, top)
_SIZE = 10  # 10 × 10 pixels
_H3_MIN, _H3_MAX = 0, 15


def _make_raster(path: str) -> None:
    make_raster(path, _BOUNDS, _SIZE, pixel_value=1.0)


def _one_dggs_per_indexer_module():
    """One representative DGGS name per distinct indexer module, derived from
    INDEXER_LOOKUP rather than hand-listed -- DGGAL's 16 grids all share one
    module (and its cell_area_m2 implementation), so this collapses them to
    a single representative automatically, and stays correct if the registry
    changes. This is the exact function that had confirmed real bugs for
    DGGAL (swapped GeoPoint(lon, lat) args) and A5 (aperture-4/5 mismatch at
    resolution 0), both previously invisible here because this test only ever
    instantiated H3RasterIndexer."""
    seen_modules = set()
    cases = []
    for name, (module_path, class_name, _extra) in INDEXER_LOOKUP.items():
        if module_path in seen_modules:
            continue
        seen_modules.add(module_path)
        cases.append((name, module_path, class_name))
    return cases


_SPEC_BY_NAME = {s.name: s for s in SPECS}
_DGGS_CASES = _one_dggs_per_indexer_module()


@pytest.fixture(params=_DGGS_CASES, ids=[c[0] for c in _DGGS_CASES])
def cell_area_indexer(request):
    dggs, module_path, class_name = request.param
    try:
        mod = importlib.import_module(module_path)
        cls = getattr(mod, class_name)
    except ImportError:
        pytest.skip(f"{dggs} extra not installed")
    spec = _SPEC_BY_NAME[dggs]
    return cls(dggs), spec.min_res, spec.max_res


class TestCellAreaM2:
    """cell_area_m2 returns positive, monotonically decreasing values, for
    every DGGS implementation (not just H3)."""

    _lat, _lon = -41.05, 174.05

    def test_positive_at_all_resolutions(self, cell_area_indexer):
        indexer, min_res, max_res = cell_area_indexer
        for res in range(min_res, max_res + 1):
            area = indexer.cell_area_m2(res, self._lat, self._lon)
            assert area > 0, f"{indexer.dggs} res {res} area must be positive"

    def test_decreases_with_resolution(self, cell_area_indexer):
        indexer, min_res, max_res = cell_area_indexer
        areas = [
            indexer.cell_area_m2(res, self._lat, self._lon)
            for res in range(min_res, max_res + 1)
        ]
        for i in range(len(areas) - 1):
            assert areas[i] > areas[i + 1], (
                f"{indexer.dggs} area should decrease from res {min_res + i} "
                f"to res {min_res + i + 1}"
            )

    def test_coarsest_larger_than_finest(self, cell_area_indexer):
        indexer, min_res, max_res = cell_area_indexer
        coarsest = indexer.cell_area_m2(min_res, self._lat, self._lon)
        finest = indexer.cell_area_m2(max_res, self._lat, self._lon)
        assert coarsest > finest


class TestComputePixelAreaM2(TestRunthrough):
    """compute_pixel_area_m2 returns a positive area and plausible centre."""

    def setUp(self):
        super().setUp()
        self._tmp = self.make_temp_raster(_make_raster)

    def test_returns_positive_area(self):
        area, _, _ = common.compute_pixel_area_m2(self._tmp)
        self.assertGreater(area, 0)

    def test_centre_within_bounds(self):
        left, bottom, right, top = _BOUNDS
        _, clat, clon = common.compute_pixel_area_m2(self._tmp)
        self.assertGreaterEqual(clat, bottom)
        self.assertLessEqual(clat, top)
        self.assertGreaterEqual(clon, left)
        self.assertLessEqual(clon, right)

    def test_area_plausible_for_pixel_size(self):
        # 0.01° × 0.01° at ~41°S → roughly 600 000 – 1 200 000 m²
        area, _, _ = common.compute_pixel_area_m2(self._tmp)
        self.assertGreater(area, 5e5)
        self.assertLess(area, 2e6)


class TestResolveModeInvariants(TestRunthrough):
    """
    Each mode must satisfy its defining property against the pixel area of the
    test raster.  We check invariants rather than hard-coded resolutions so the
    tests don't break if Earth-model constants are refined.
    """

    def setUp(self):
        super().setUp()
        self._tmp = self.make_temp_raster(_make_raster)
        self.indexer = H3RasterIndexer("h3")
        self.pixel_area, self.clat, self.clon = common.compute_pixel_area_m2(self._tmp)

    def _resolve(self, mode):
        return common.resolve_resolution_mode(mode, "h3", self._tmp, _H3_MIN, _H3_MAX)

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
        self._tmp = self.make_temp_raster(_make_raster)

    def _run(self, mode):
        return self.invoke_cli("h3", self._tmp, TEST_OUTPUT_PATH, mode)

    def test_all_modes_exit_zero(self):
        for mode in ResolutionMode:
            with self.subTest(mode=mode):
                self._run(mode)

    def test_invalid_mode_string_exits_nonzero(self):
        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["h3", self._tmp, str(TEST_OUTPUT_PATH), "-r", "not-a-mode"],
        )
        self.assertNotEqual(result.exit_code, 0)

    def test_invalid_integer_out_of_range_exits_nonzero(self):
        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["h3", self._tmp, str(TEST_OUTPUT_PATH), "-r", "99"],
        )
        self.assertNotEqual(result.exit_code, 0)
