"""
Raster assign_centers context for --transfer assign_centers (the default).

_AssignCentersIndexer holds all shared state and exposes process_window, called
once per raster window in a Stage 1 worker process.
"""

from __future__ import annotations

import dataclasses
import threading
from collections.abc import Callable
from typing import Any

import numpy as np
import pyproj
import rasterio as rio
import xarray as xr

from raster2dggs.interfaces import IRasterIndexer
from raster2dggs.profiling import PROFILER


@dataclasses.dataclass(repr=False)
class _AssignCentersIndexer:
    """Shared context for --transfer assign_centers.

    Instantiate once per worker process; call ctx.process_window per window.
    """

    src: rio.DatasetReader
    indexer: IRasterIndexer
    resolution: int
    parent_res: int
    nodata: Any
    selected_labels: tuple
    selected_indices: tuple
    nodata_policy: str
    emit_nodata_value: Any | None
    transformer: pyproj.Transformer
    write_result: Callable
    # Set when the source carries an alpha/mask band and --mask is on.
    apply_mask: bool = False
    # Lock guarding reads of ``src``: a GDAL dataset is not safe for
    # concurrent access.
    read_lock: Any = None

    def __post_init__(self):
        self._read_lock = self.read_lock or threading.Lock()

    def process_window(self, window):
        """Index all pixels in this raster window to their containing DGGS cell."""
        indexes = list(self.selected_indices)
        valid_mask = None
        with PROFILER.phase("stage1.read_block"), self._read_lock:
            # Exactly this window, and nothing around it: the read is the only
            # IO Stage 1 does per window, so its size is the cost per window.
            values = self.src.read(indexes=indexes, window=window)
        if self.apply_mask:
            with PROFILER.phase("stage1.read_mask"), self._read_lock:
                valid_mask = self.src.read_masks(indexes=indexes, window=window) != 0
        result = self.indexer.index_func(
            window_block(values, indexes, window, self.src.transform),
            self.resolution,
            self.parent_res,
            self.nodata,
            band_labels=self.selected_labels,
            nodata_policy=self.nodata_policy,
            emit_nodata_value=self.emit_nodata_value,
            transformer=self.transformer,
            selected_indices=self.selected_indices,
            valid_mask=valid_mask,
        )
        self.write_result(result, window)


def window_block(
    values: np.ndarray, indexes: list[int], window: rio.windows.Window, transform
) -> xr.DataArray:
    """A (band, y, x) block with pixel-centre coordinates, as ``index_func``
    expects it.

    Coordinates are computed from the dataset transform and the window's
    global pixel offsets, so they are bit-identical to those of the full
    raster: a window-local transform would carry the offset into the affine
    constants and could differ in the last bit.
    """
    centre = transform * transform.translation(0.5, 0.5)
    cols = np.arange(window.col_off, window.col_off + window.width)
    rows = np.arange(window.row_off, window.row_off + window.height)
    xs, _ = centre * (cols, np.zeros(window.width))
    _, ys = centre * (np.zeros(window.height), rows)
    return xr.DataArray(
        values,
        dims=("band", "y", "x"),
        coords={"band": np.asarray(indexes), "y": ys, "x": xs},
    )
