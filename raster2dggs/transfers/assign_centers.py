"""
Raster assign_centers context for --transfer assign_centers (the default).

_AssignCentersIndexer holds all shared state and exposes process_window as a bound
method callable by ThreadPoolExecutor.map.
"""

from __future__ import annotations

import dataclasses
from typing import Any, Callable, Optional

import pyproj
import xarray as xr

from raster2dggs.interfaces import IRasterIndexer


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
    nodata_policy: str
    emit_nodata_value: Optional[Any]
    transformer: pyproj.Transformer
    write_result: Callable

    def process_window(self, window):
        """Index all pixels in this raster window to their containing DGGS cell."""
        sdf = self.da.rio.isel_window(window)
        result = self.indexer.index_func(
            sdf,
            self.resolution,
            self.parent_res,
            self.nodata,
            band_labels=self.selected_labels,
            nodata_policy=self.nodata_policy,
            emit_nodata_value=self.emit_nodata_value,
            transformer=self.transformer,
        )
        self.write_result(result, window)
        return None
