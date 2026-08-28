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

    da: xr.DataArray
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
    # The dataset behind ``da``, needed only to read its mask bands. apply_mask
    # is set when the source carries an alpha/mask band and --mask is on.
    src: rio.DatasetReader | None = None
    apply_mask: bool = False
    # Lock guarding reads of ``src``; share the one rioxarray uses for ``da``
    # (the same GDAL dataset), which is not safe for concurrent access.
    read_lock: Any = None

    def __post_init__(self):
        self._read_lock = self.read_lock or threading.Lock()

    def process_window(self, window):
        """Index all pixels in this raster window to their containing DGGS cell."""
        sdf = self.da.rio.isel_window(window)
        valid_mask = None
        if self.apply_mask:
            # Read in the DataArray's own band order, not selected_indices
            # order: when every band is selected the DataArray is left in
            # natural order regardless of the order -b was given in.
            band_order = [int(b) for b in sdf["band"].values]
            with PROFILER.phase("stage1.read_mask"), self._read_lock:
                valid_mask = self.src.read_masks(indexes=band_order, window=window) != 0
        result = self.indexer.index_func(
            sdf,
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
