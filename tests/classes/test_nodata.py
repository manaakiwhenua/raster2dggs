from unittest import TestCase

import numpy as np
import pandas as pd
from classes.base import TestRunthrough, read_output
from classes.helpers import make_raster
from data.datapaths import TEST_OUTPUT_PATH
from raster2dggs.indexers.rasterindexer import _mask_is_nodata

NODATA_SENTINEL = -9999.0
# 10x10 pixel Float32 raster in EPSG:4326; top-left pixel is nodata
RASTER_BOUNDS = (174.0, -41.1, 174.1, -41.0)  # (left, bottom, right, top)
RASTER_SIZE = 10
H3_RES = 7  # fine enough that each pixel maps to its own cell


def _make_test_raster(path: str, nodata: float = NODATA_SENTINEL) -> None:
    make_raster(path, RASTER_BOUNDS, RASTER_SIZE, pixel_value=42.0, nodata=nodata)


class TestMaskIsNodata(TestCase):
    def test_nodata_none_returns_all_false(self):
        s = pd.Series([1.0, 2.0, np.nan])
        self.assertFalse(_mask_is_nodata(s, nodata=None).any())

    def test_nodata_nan_masks_nans_only(self):
        s = pd.Series([1.0, np.nan, 3.0])
        result = _mask_is_nodata(s, nodata=np.nan)
        self.assertEqual(result.tolist(), [False, True, False])

    def test_nodata_nan_does_not_mask_valid(self):
        s = pd.Series([1.0, 2.0, 3.0])
        self.assertFalse(_mask_is_nodata(s, nodata=np.nan).any())

    def test_nodata_sentinel_masks_matching_values(self):
        s = pd.Series([1.0, NODATA_SENTINEL, 3.0])
        result = _mask_is_nodata(s, nodata=NODATA_SENTINEL)
        self.assertEqual(result.tolist(), [False, True, False])

    def test_nodata_sentinel_also_masks_nans(self):
        s = pd.Series([1.0, NODATA_SENTINEL, np.nan])
        result = _mask_is_nodata(s, nodata=NODATA_SENTINEL)
        self.assertEqual(result.tolist(), [False, True, True])

    def test_nodata_sentinel_does_not_mask_other_values(self):
        s = pd.Series([0.0, 1.0, 2.0])
        self.assertFalse(_mask_is_nodata(s, nodata=NODATA_SENTINEL).any())


class TestNodataPolicy(TestRunthrough):
    def setUp(self):
        super().setUp()
        self._raster = self.make_temp_raster(_make_test_raster)

    def _run(self, *extra_args):
        self.invoke_cli("h3", self._raster, TEST_OUTPUT_PATH, H3_RES, *extra_args)
        return read_output(TEST_OUTPUT_PATH).to_pandas()

    def test_omit_excludes_nodata_cells(self):
        df = self._run("--nodata_policy", "omit")
        self.assertFalse(
            (df["band_1"] == NODATA_SENTINEL).any(),
            "omit policy should produce no nodata-sentinel values",
        )
        self.assertFalse(
            df["band_1"].isna().any(), "omit policy should produce no NaN values"
        )

    def test_emit_with_explicit_value_lowers_cell_value(self):
        # All valid pixels have value 42. The nodata pixel is replaced with 0,
        # so any H3 cell containing it will have a mean < 42 (or exactly 0 if isolated).
        df_omit = self._run("--nodata_policy", "omit")
        omit_min = df_omit["band_1"].min()
        df_emit = self._run("--nodata_policy", "emit", "--emit_nodata_value", "0")
        self.assertLess(
            df_emit["band_1"].min(),
            omit_min,
            "replacing nodata with 0 should pull at least one cell's mean below 42",
        )

    def test_emit_without_explicit_value_includes_nodata_cells(self):
        df_omit = self._run("--nodata_policy", "omit")
        df_emit = self._run("--nodata_policy", "emit")
        self.assertGreaterEqual(
            len(df_emit),
            len(df_omit),
            "emit policy should produce at least as many rows as omit",
        )
