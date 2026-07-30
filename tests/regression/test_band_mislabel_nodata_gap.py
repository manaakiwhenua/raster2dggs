"""
Regression test for issue #88: a band entirely nodata within one raster
window could silently mislabel every later band's data under the wrong
column name.

Background: `RasterIndexer.index_func`'s `--nodata omit` (default) policy
drops all-nodata rows from the working dataframe before computing which
bands are actually present in this window (`bands = sorted(sdf["band"].
unique())`). If an entire band is nodata within this window, it vanishes
from `bands` -- but `band_labels` (the full, fixed set of globally
selected bands) doesn't shrink to match. The old code paired the two
positionally (`zip(bands, band_labels)`), so every band after the dropped
one got shifted onto the wrong label.

Fixed by pairing band_labels against selected_indices (the fixed, global
band-index list they were built from) to get a {band_index: label}
mapping, then applying that by band index rather than by position -- so a
window with fewer bands present just renames fewer columns, instead of
shifting the remaining ones.
"""

import numpy as np
import rasterio
from classes.base import TestRunthrough, read_output
from data.datapaths import TEST_OUTPUT_PATH
from rasterio.crs import CRS
from rasterio.transform import from_bounds

_BOUNDS = (174.0, -41.1, 174.1, -41.0)
_SIZE = 4
_NODATA = -9999.0
_BAND_1_VALUE = 10.0
_BAND_3_VALUE = 30.0


def _make_raster_with_middle_band_nodata(path: str) -> None:
    """3-band raster: band 1 and band 3 have real data; band 2 is entirely
    nodata, so it's dropped entirely from the (--nodata omit default)
    output -- reproducing the exact conditions of issue #88."""
    data = np.empty((3, _SIZE, _SIZE), dtype=np.float32)
    data[0] = _BAND_1_VALUE
    data[1] = _NODATA
    data[2] = _BAND_3_VALUE
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        height=_SIZE,
        width=_SIZE,
        count=3,
        dtype="float32",
        crs=CRS.from_epsg(4326),
        transform=from_bounds(*_BOUNDS, _SIZE, _SIZE),
        nodata=_NODATA,
    ) as dst:
        dst.write(data)


class TestBandMislabelNodataGap(TestRunthrough):
    def setUp(self):
        super().setUp()
        self._raster = self.make_temp_raster(_make_raster_with_middle_band_nodata)

    def test_bands_are_not_mislabelled(self):
        self.invoke_cli("h3", self._raster, TEST_OUTPUT_PATH, 4, "--agg", "mean")
        table = read_output(TEST_OUTPUT_PATH)
        df = table.to_pandas()

        self.assertIn("band_1", df.columns)
        self.assertIn("band_3", df.columns)
        self.assertNotIn("band_2", df.columns)

        self.assertTrue((df["band_1"] == _BAND_1_VALUE).all())
        self.assertTrue((df["band_3"] == _BAND_3_VALUE).all())
