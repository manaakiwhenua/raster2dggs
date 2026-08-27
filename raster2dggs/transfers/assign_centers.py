"""
Raster assign_centers context for --transfer assign_centers (the default).

_AssignCentersIndexer holds all shared state and exposes process_window as a bound
method callable by ThreadPoolExecutor.map.
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

    Instantiate once; pass ctx.process_window to ThreadPoolExecutor.map.
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

    def __post_init__(self):
        # ``src`` is one GDAL dataset shared by every worker thread and is not
        # safe for concurrent access.
        self._read_lock = threading.Lock()

    def process_window(self, window):
        """Index all pixels in this raster window to their containing DGGS cell."""
        sdf = self.da.rio.isel_window(window)
        valid_mask = None
        if self.apply_mask:
            with PROFILER.phase("stage1.read_mask"), self._read_lock:
                valid_mask = (
                    self.src.read_masks(
                        indexes=list(self.selected_indices), window=window
                    )
                    != 0
                )
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
