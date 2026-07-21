"""
Regression test for https://github.com/manaakiwhenua/raster2dggs/issues/69.

The issue claims that rioxarray's unconditional +0.5-pixel offset makes
--point coordinates wrong for rasters tagged AREA_OR_POINT=Point, and
proposes subtracting a half-pixel offset whenever that tag is present.

Empirically, this does not hold for GDAL-read GeoTIFFs: GDAL's GeoTIFF
driver normalises Point-tagged files on write and read specifically so
that rasterio's `.transform` (and therefore rioxarray's blanket +0.5) lands
on the true sample location regardless of the tag. Two rasters written with
the *same* transform, differing only in the AREA_OR_POINT tag, therefore
represent the same physical pixel grid and must produce identical output.

This test locks in that (correct) behaviour, so that a future "fix" for
#69 based on the issue's proposed unconditional half-pixel subtraction
would be caught here before it reintroduces a real bug.
"""

import numpy as np
import rasterio
from rasterio.transform import from_origin

from classes.base import TestRunthrough, read_output
from data.datapaths import TEST_OUTPUT_PATH

_BOUNDS_ORIGIN = (174.0, -41.0)  # (left, top)
_PIXEL_SIZE = 0.01
_SIZE = 4
_RES = 9


def _write_tagged_raster(path: str, tag: str) -> None:
    data = np.arange(_SIZE * _SIZE, dtype=np.float32).reshape(1, _SIZE, _SIZE)
    transform = from_origin(*_BOUNDS_ORIGIN, _PIXEL_SIZE, _PIXEL_SIZE)
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        height=_SIZE,
        width=_SIZE,
        count=1,
        dtype="float32",
        crs="EPSG:4326",
        transform=transform,
    ) as dst:
        dst.write(data)
        dst.update_tags(AREA_OR_POINT=tag)


class TestAreaOrPointDoesNotAffectPointOutput(TestRunthrough):
    """--point output must be identical for Area- and Point-tagged rasters
    that share the same underlying transform."""

    def _run(self, tag: str):
        raster = self.make_temp_raster(lambda p: _write_tagged_raster(p, tag))
        self.invoke_cli("h3", raster, TEST_OUTPUT_PATH, _RES, "--point", "value")
        table = read_output(TEST_OUTPUT_PATH)
        df = table.to_pandas().sort_index()
        return df

    def test_area_and_point_tags_produce_identical_cells_and_values(self):
        area_df = self._run("Area")
        point_df = self._run("Point")
        self.assertListEqual(
            area_df.index.tolist(),
            point_df.index.tolist(),
            "AREA_OR_POINT tag must not change which cells pixels are assigned to",
        )
        self.assertListEqual(
            area_df["band_1"].tolist(),
            point_df["band_1"].tolist(),
            "AREA_OR_POINT tag must not change assigned pixel values",
        )
