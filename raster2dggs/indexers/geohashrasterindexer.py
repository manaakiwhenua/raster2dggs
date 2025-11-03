"""
@author: ndemaio
"""

from numbers import Number
from typing import Tuple

import geohash as gh
import pandas as pd
import pyarrow as pa
import xarray as xr
import numpy as np
import shapely

import raster2dggs.constants as const

from raster2dggs.indexers.rasterindexer import RasterIndexer


class GeohashRasterIndexer(RasterIndexer):
    """
    Provides integration for the Geohash geocode system.
    """

    def index_func(
        self,
        sdf: xr.DataArray,
        precision: int,
        parent_precision: int,
        nodata: Number = np.nan,
        band_labels: Tuple[str] = None,
    ) -> pa.Table:
        """
        Index a raster window to Geohash.
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
        # Primary Geohash index
        geohash = [
            gh.encode(lat, lon, precision=precision)
            for lat, lon in zip(subset["y"], subset["x"])
        ]  # Vectorised
        # Secondary (parent) Geohash index, used later for partitioning
        geohash_parent = [gh[:parent_precision] for gh in geohash]
        subset = subset.drop(columns=["x", "y"])
        index_col = self.index_col(precision)
        subset[index_col] = pd.Series(geohash, index=subset.index)
        partition_col = self.partition_col(parent_precision)
        subset[partition_col] = pd.Series(geohash_parent, index=subset.index)
        # Rename bands
        bands = sdf["band"].unique()
        columns = dict(zip(bands, band_labels))
        subset = subset.rename(columns=columns)
        return pa.Table.from_pandas(subset)

    @staticmethod
    def cell_to_children_size(cell, desired_level: int) -> int:
        """
        Determine total number of children at some offset resolution

        Implementation of interface function.
        """
        level = len(cell)
        if desired_level < level:
            return 0
        return 32 ** (desired_level - level)

    @staticmethod
    def valid_set(cells: set) -> set[str]:
        """
        Implementation of interface function.
        """
        return set(filter(lambda c: not pd.isna(c), cells))

    def parent_cells(self, cells: set, precision) -> map:
        """
        Implementation of interface function.
        """
        return map(lambda gh: self.to_parent(gh, precision), cells)

    def expected_count(self, parent: str, precision: int):
        """
        Implementation of interface function.
        """
        return self.cell_to_children_size(parent, precision)

    @staticmethod
    def to_parent(cell: str, desired_precision: int) -> str:
        """
        Returns cell parent at some offset level.

        Not a part of the RasterIndexer interface
        """
        return cell[:desired_precision]

    @staticmethod
    def cell_to_point(cell: str) -> shapely.geometry.Point:
        lat, lon, lat_err, lon_err = gh.decode_exactly(cell)
        return shapely.Point(lon, lat)

    @staticmethod
    def cell_to_polygon(cell: str) -> shapely.geometry.Polygon:
        lat, lon, lat_err, lon_err = gh.decode_exactly(cell)
        return shapely.geometry.box(
            lon - lon_err, lat - lat_err, lon + lon_err, lat + lat_err
        )
