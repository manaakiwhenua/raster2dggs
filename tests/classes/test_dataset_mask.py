"""
GDAL dataset masks (alpha bands, internal mask bands) honoured as nodata -- #105.

Fixture rasters are 20x20 with the left half masked (alpha 0 / mask 0, pixel
value 0) and the right half valid with a constant value. Any cell whose value
differs from that constant has had masked pixels averaged in, so "every value
equals the constant" is the whole assertion for the masked runs, while the
--no-mask runs must show the black filler leaking through.
"""

import numpy as np
import rasterio
from classes.base import TestRunthrough, read_output
from classes.helpers import make_internal_mask_raster, make_raster, make_rgba_raster
from data.datapaths import TEST_OUTPUT_PATH

from raster2dggs import common

_BOUNDS = (174.0, -41.1, 174.1, -41.0)
_SIZE = 20
_VALUE = 100
# H3 res 7 cells (~5 km²) hold ~16 of these 0.005° pixels, so cells straddle
# the mask edge; res 9 cells hold about one pixel, exercising --sample.
_RES = 7
_SAMPLE_RES = 9


def _rgba(path):
    make_rgba_raster(path, _BOUNDS, _SIZE, pixel_value=_VALUE)


def _internal_mask(path):
    make_internal_mask_raster(path, _BOUNDS, _SIZE, pixel_value=1.0)


class TestNeedsMaskRead(TestRunthrough):
    def _check(self, make_func, expected):
        path = self.make_temp_raster(make_func)
        with rasterio.open(path) as src:
            self.assertEqual(
                common._needs_mask_read(src, range(1, src.count + 1)), expected
            )

    def test_alpha_band_needs_mask(self):
        self._check(_rgba, True)

    def test_internal_mask_needs_mask(self):
        self._check(_internal_mask, True)

    def test_declared_nodata_only_does_not(self):
        self._check(lambda p: make_raster(p, _BOUNDS, _SIZE, nodata=-9999.0), False)

    def test_all_valid_does_not(self):
        self._check(lambda p: make_raster(p, _BOUNDS, _SIZE), False)


class TestAlphaMask(TestRunthrough):
    def setUp(self):
        super().setUp()
        self._raster = self.make_temp_raster(_rgba)

    def _run(self, *extra_args, res=_RES):
        self.invoke_cli("h3", self._raster, TEST_OUTPUT_PATH, res, *extra_args)
        return read_output(TEST_OUTPUT_PATH).to_pandas()

    def _assert_only_valid_values(self, df, bands=("band_1", "band_2", "band_3")):
        self.assertGreater(len(df), 0)
        for b in bands:
            vals = df[b].to_numpy(dtype=float)
            self.assertTrue(
                np.all(vals == _VALUE), f"{b}: masked pixels leaked in: {vals}"
            )

    # --point (default)
    def test_point_masks_pixels_and_consumes_alpha(self):
        df = self._run()
        self.assertNotIn("band_4", df.columns)
        self._assert_only_valid_values(df)

    def test_point_no_mask_emits_alpha_and_filler(self):
        df = self._run("--no-mask")
        self.assertIn("band_4", df.columns)
        self.assertLess(df["band_1"].min(), _VALUE)  # black filler averaged in
        self.assertLess(df["band_4"].min(), 255)

    def test_point_explicit_alpha_band_is_emitted_as_data(self):
        # Selected explicitly, the alpha band is data and is always valid, so
        # under 'omit' a masked pixel still yields a row: NaN in the masked
        # bands, alpha 0 -- i.e. a per-cell coverage feature.
        df = self._run("-b", "1", "-b", "4")
        self.assertIn("band_4", df.columns)
        b1 = df["band_1"].to_numpy(dtype=float)
        b4 = df["band_4"].to_numpy(dtype=float)
        self.assertTrue(np.all(b1[~np.isnan(b1)] == _VALUE))
        self.assertTrue(np.isnan(b1).any())  # fully masked cells survive via alpha
        self.assertTrue(np.all(b4[np.isnan(b1)] == 0))
        self.assertTrue(np.all(b1[b4 == 255] == _VALUE))

    def test_point_emit_writes_fill_for_masked(self):
        df = self._run("-n", "emit", "--nodata-fill", "-1", "-d", "0")
        vals = df["band_1"].to_numpy(dtype=float)
        self.assertTrue((vals == -1).any())  # fully masked cells
        self.assertTrue((vals == _VALUE).any())  # fully valid cells

    def test_point_masked_run_has_fewer_cells(self):
        n_masked = len(self._run())
        n_raw = len(self._run("--no-mask"))
        self.assertLess(n_masked, n_raw)

    # --sample
    def test_sample_nn_masks(self):
        df = self._run("--sample", "nn", res=_SAMPLE_RES)
        self._assert_only_valid_values(df)

    def test_sample_bilinear_masks(self):
        df = self._run("--sample", "bilinear", res=_SAMPLE_RES)
        self._assert_only_valid_values(df)

    def test_sample_no_mask_leaks_filler(self):
        df = self._run("--sample", "nn", "--no-mask", res=_SAMPLE_RES)
        self.assertLess(df["band_1"].min(), _VALUE)

    # --overlay
    def test_overlay_weighted_masks(self):
        df = self._run("--overlay", "weighted")
        self._assert_only_valid_values(df)

    def test_overlay_no_mask_leaks_filler(self):
        df = self._run("--overlay", "weighted", "--no-mask")
        self.assertLess(df["band_1"].min(), _VALUE)

    def test_overlay_coverage_threshold_counts_masked_as_invalid(self):
        n_all = len(self._run("--overlay", "weighted"))
        n_vct = len(self._run("--overlay", "weighted", "-vct", "0.9"))
        self.assertLess(n_vct, n_all)  # edge cells straddling the mask are dropped


class TestInternalMask(TestRunthrough):
    def setUp(self):
        super().setUp()
        self._raster = self.make_temp_raster(_internal_mask)

    def _run(self, *extra_args):
        self.invoke_cli("h3", self._raster, TEST_OUTPUT_PATH, _RES, *extra_args)
        return read_output(TEST_OUTPUT_PATH).to_pandas()

    def test_point_masks_pixels(self):
        df = self._run()
        self.assertGreater(len(df), 0)
        self.assertTrue(np.all(df["band_1"].to_numpy(dtype=float) == 1.0))

    def test_point_no_mask_leaks_filler(self):
        df = self._run("--no-mask")
        self.assertLess(df["band_1"].min(), 1.0)
