"""
@author: ndemaio
"""

from numbers import Number
from typing import Tuple

import h3pandas  # Necessary import despite lack of explicit use

import h3 as h3py
import pandas as pd
import pyarrow as pa
import xarray as xr
import numpy as np
import shapely

import raster2dggs.constants as const

from raster2dggs.indexers.rasterindexer import RasterIndexer


class H3RasterIndexer(RasterIndexer):
    """
    Provides integration for Uber's H3 DGGS.
    """

    def index_func(
        self,
        sdf: xr.DataArray,
        resolution: int,
        parent_res: int,
        nodata: Number = np.nan,
        band_labels: Tuple[str] = None,
    ) -> pa.Table:
        """
        Index a raster window to H3.
        Subsequent steps are necessary to resolve issues at the boundaries of windows.
        If windows are very small, or in strips rather than blocks, processing may be slower
        than necessary and the recommendation is to write different windows in the source raster.

        Implementation of interface function.
        """
        sdf: pd.DataFrame = (
            sdf.to_dataframe().drop(columns=["spatial_ref"]).reset_index()
        )
        subset: pd.DataFrame = sdf.dropna()
        subset = subset[subset.value != nodata]
        subset = pd.pivot_table(
            subset, values=const.DEFAULT_NAME, index=["x", "y"], columns=["band"]
        ).reset_index()
        # Primary H3 index
        h3index = subset.h3.geo_to_h3(resolution, lat_col="y", lng_col="x").drop(
            columns=["x", "y"]
        )
        # Secondary (parent) H3 index, used later for partitioning
        h3index = h3index.h3.h3_to_parent(parent_res).reset_index()
        # Renaming columns to actual band labels
        bands = sdf["band"].unique()
        columns = dict(zip(bands, band_labels))
        h3index = h3index.rename(columns=columns)
        return pa.Table.from_pandas(h3index)

    @staticmethod
    def cell_to_children_size(cell, desired_resolution: int) -> int:
        """
        Determine total number of children at some offset resolution

        Implementation of interface function.
        """
        current_resolution = h3py.get_resolution(cell)
        n = desired_resolution - current_resolution
        if h3py.is_pentagon(cell):
            return 1 + 5 * (7**n - 1) // 6
        else:
            return 7**n

    @staticmethod
    def valid_set(cells: set) -> set[str]:
        """
        Implementation of interface function.
        """
        return set(filter(lambda c: (not pd.isna(c)) and h3py.is_valid_cell(c), cells))

    @staticmethod
    def parent_cells(cells: set, resolution) -> map:
        """
        Implementation of interface function.
        """
        return map(lambda x: h3py.cell_to_parent(x, resolution), cells)

    def expected_count(self, parent: str, resolution: int):
        """
        Implementation of interface function.
        """
        return self.cell_to_children_size(parent, resolution)

    @staticmethod
    def cell_to_point(cell: str) -> shapely.geometry.Point:
        return shapely.Point(h3py.cell_to_latlng(cell)[::-1])

    @staticmethod
    def cell_to_polygon(cell: str) -> shapely.geometry.Polygon:
        return shapely.Polygon(
            tuple(coord[::-1] for coord in h3py.cell_to_boundary(cell))
        )
