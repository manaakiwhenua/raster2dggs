"""
A Stage 1 window read must touch only that window's pixels.

The point transfer once read each window by slicing a dask-backed array whose
chunks were sized by memory ("auto"), not by the raster's blocks. With every
band selected, dask fused the window slice into the read and only the window
was fetched. With a band *subset* (``--band 1 --band 2 --band 3`` on RGBA
imagery, say) the band selection sat between the read and the slice, nothing
fused, and each window read materialised its whole chunk -- up to 128 MiB,
most of a mid-sized raster -- relying on the per-worker GDAL block cache to
make the next window cheap. When that cache was smaller than the chunk (it
shrinks as --processes grows), every window re-read the region; over
/vsicurl/ that re-downloaded most of the dataset per window (issue #120).
Every output was still correct, so only the cost showed.

The read is intercepted at ``DatasetReader.read`` because both the old and the
new paths go through it; the assertion is on the shape of what was requested,
which is independent of cache size, link speed or process count.
"""

import numpy as np
import pytest
import rasterio
from click.testing import CliRunner
from rasterio.crs import CRS
from rasterio.transform import from_bounds
from rasterio.windows import Window

from raster2dggs.cli import cli

_BLOCK = 256
_BANDS = 3
_SIZE = 2 * _BLOCK  # four blocks, so "one block" and "the raster" differ
_BOUNDS = (174.0, -41.2, 174.2, -41.0)


@pytest.fixture(scope="module")
def tiled_raster(tmp_path_factory):
    path = tmp_path_factory.mktemp("tiled") / "tiled.tif"
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        height=_SIZE,
        width=_SIZE,
        count=_BANDS,
        dtype="float32",
        crs=CRS.from_epsg(4326),
        transform=from_bounds(*_BOUNDS, _SIZE, _SIZE),
        tiled=True,
        blockxsize=_BLOCK,
        blockysize=_BLOCK,
    ) as dst:
        dst.write(np.full((_BANDS, _SIZE, _SIZE), 1.0, dtype="float32"))
    with rasterio.open(path) as src:
        assert src.block_shapes[0] == (_BLOCK, _BLOCK)
    return str(path)


@pytest.fixture
def read_windows(monkeypatch):
    """Every window handed to ``DatasetReader.read`` in this process."""
    seen: list[Window] = []
    original = rasterio.io.DatasetReader.read

    def recording(self, *args, **kwargs):
        window = kwargs.get("window")
        if window is not None:
            # rioxarray passes ((row0, row1), (col0, col1)) tuples.
            if not isinstance(window, Window):
                window = Window.from_slices(*window)
            seen.append(window)
        return original(self, *args, **kwargs)

    monkeypatch.setattr(rasterio.io.DatasetReader, "read", recording)
    return seen


@pytest.mark.parametrize(
    "band_flags",
    [
        pytest.param([], id="all-bands"),
        pytest.param(["--band", "1", "--band", "2"], id="band-subset"),
    ],
)
def test_point_transfer_reads_no_more_than_one_block_per_window(
    tiled_raster, read_windows, tmp_path, band_flags
):
    out = tmp_path / "out"
    result = CliRunner().invoke(
        cli,
        [
            "h3",
            tiled_raster,
            str(out),
            "-r",
            "7",
            "--point",
            "value",
            "--processes",
            "1",  # inline, so the reads happen in this process
            "--overwrite",
            *band_flags,
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0, result.output

    assert read_windows, "no windowed read was intercepted"
    largest = max(read_windows, key=lambda w: w.width * w.height)
    assert largest.width * largest.height <= _BLOCK * _BLOCK, (
        f"a window read requested {largest.width}x{largest.height} pixels; "
        f"blocks are {_BLOCK}x{_BLOCK}"
    )
