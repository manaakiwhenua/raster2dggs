"""
@author: ndemaio
"""

from numbers import Number
from typing import Tuple

import rhppandas  # Necessary import despite lack of explicit use

import rhealpixdggs.rhp_wrappers as rhpw
import pandas as pd
import pyarrow as pa
import xarray as xr
import numpy as np
import shapely
from rhealpixdggs.dggs import WGS84_003

import raster2dggs.constants as const

from raster2dggs.indexers.rasterindexer import RasterIndexer


class RHPRasterIndexer(RasterIndexer):
    """
    Provides integration for MWLR's rHEALPix DGGS.
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
        Index a raster window to rHEALPix.
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
        # Primary rHEALPix index
        rhpindex = subset.rhp.geo_to_rhp(resolution, lat_col="y", lng_col="x").drop(
            columns=["x", "y"]
        )
        # Secondary (parent) rHEALPix index, used later for partitioning
        rhpindex = rhpindex.rhp.rhp_to_parent(parent_res).reset_index()
        # Renaming columns to actual band labels
        bands = sdf["band"].unique()
        columns = dict(zip(bands, band_labels))
        rhpindex = rhpindex.rename(columns=columns)
        return pa.Table.from_pandas(rhpindex)

    @staticmethod
    def cell_to_children_size(cell, desired_resolution: int) -> int:
        """
        Determine total number of children at some offset resolution

        Implementation of interface function.
        """
        if desired_resolution < len(cell):
            return 0
        if len(cell) == 1:  # Level 0 has 6 faces, each then divides into 9
            return 6 * (9 ** (desired_resolution - 1))
        return 9 ** (desired_resolution - len(cell) + 1)

    @staticmethod
    def valid_set(cells: set) -> set[str]:
        """
        Implementation of interface function.
        """
        return set(filter(lambda c: not pd.isna(c), cells))

    @staticmethod
    def parent_cells(cells: set, resolution) -> map:
        """
        Implementation of interface function.
        """
        return map(lambda x: rhpw.rhp_to_parent(x, resolution), cells)

    def expected_count(self, parent: str, resolution: int):
        """
        Implementation of interface function.
        """
        return self.cell_to_children_size(parent, resolution)

    @staticmethod
    def cell_to_point(cell: str) -> shapely.geometry.Point:
        return shapely.Point(rhpw.rhp_to_geo(cell, plane=False, dggs=WGS84_003))

    @staticmethod
    def cell_to_polygon(cell: str) -> shapely.geometry.Polygon:
        return shapely.Polygon(
            tuple(
                coord
                for coord in rhpw.rhp_to_geo_boundary(cell, plane=False, dggs=WGS84_003)
            )
        )
