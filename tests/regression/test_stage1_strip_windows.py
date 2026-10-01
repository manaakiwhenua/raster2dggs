"""
Strip-encoded rasters must not cost one Stage 1 window per row.

A GeoTIFF written without tiling is stored as strips, full-width GDAL blocks a
few rows tall (one row, for most real exports), and ``src.block_windows()``
yields one window per strip. Every window carries
fixed overhead -- a GDAL read, a DGGS call, a Parquet file -- so a modest
untiled raster produced thousands of tiny windows and looked hung. The README
told users to re-tile first. Block windows are now coalesced before dispatch:
runs of full-width strips are stacked into windows of roughly a tile's worth
of pixels, which is what a tiled raster would have given anyway.

The interception point and fixture style follow test_stage1_window_reads.py.
"""

import numpy as np
import pytest
import rasterio
from classes.helpers import make_raster
from click.testing import CliRunner
from rasterio.windows import Window

from raster2dggs.cli import cli

_SIZE = 256
_BOUNDS = (174.0, -41.2, 174.2, -41.0)


@pytest.fixture(scope="module")
def striped_raster(tmp_path_factory):
    path = tmp_path_factory.mktemp("strips") / "strips.tif"
    make_raster(str(path), _BOUNDS, _SIZE, pixel_value=7.0)
    with rasterio.open(path) as src:
        height, width = src.block_shapes[0]
        assert width == _SIZE and height < _SIZE, "fixture must be strip-encoded"
    return str(path)


@pytest.fixture
def read_windows(monkeypatch):
    seen: list[Window] = []
    original = rasterio.io.DatasetReader.read

    def recording(self, *args, **kwargs):
        window = kwargs.get("window")
        if window is not None:
            if not isinstance(window, Window):
                window = Window.from_slices(*window)
            seen.append(window)
        return original(self, *args, **kwargs)

    monkeypatch.setattr(rasterio.io.DatasetReader, "read", recording)
    return seen


def test_strips_are_coalesced_into_tile_sized_windows(
    striped_raster, read_windows, tmp_path
):
    out = tmp_path / "out"
    result = CliRunner().invoke(
        cli,
        [
            "h3",
            striped_raster,
            str(out),
            "-r",
            "7",
            "--point",
            "value",
            "--processes",
            "1",
            "--overwrite",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0, result.output

    # 256 x 256 pixels is a quarter of a 512 x 512 tile: one window, not one
    # per strip.
    assert len(read_windows) == 1, f"{len(read_windows)} window reads"

    # Coalescing must still cover every pixel exactly once.
    rows = np.zeros(_SIZE, dtype=int)
    for w in read_windows:
        assert w.col_off == 0 and w.width == _SIZE
        rows[w.row_off : w.row_off + w.height] += 1
    assert (rows == 1).all()
