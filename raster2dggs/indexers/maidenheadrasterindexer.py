"""
@author: ndemaio
"""

from numbers import Number
from typing import Tuple

import maidenhead as mh
import pandas as pd
import pyarrow as pa
import xarray as xr
import numpy as np
import shapely

import raster2dggs.constants as const

from raster2dggs.indexers.rasterindexer import RasterIndexer


class MaidenheadRasterIndexer(RasterIndexer):
    """
    Provides integration for Maidenhead Locator System geocodes.
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
        Index a raster window to Maidenhead.
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
        # Primary Maidenhead index
        maidenhead = [
            mh.to_maiden(lat, lon, precision)
            for lat, lon in zip(subset["y"], subset["x"])
        ]  # Vectorised
        # Secondary (parent) Maidenhead index, used later for partitioning
        maidenhead_parent = [
            self.cell_to_parent(mh, parent_precision) for mh in maidenhead
        ]
        subset = subset.drop(columns=["x", "y"])
        index_col = self.index_col(precision)
        subset[index_col] = pd.Series(maidenhead, index=subset.index)
        partition_col = self.partition_col(parent_precision)
        subset[partition_col] = pd.Series(maidenhead_parent, index=subset.index)
        # Rename bands
        bands = sdf["band"].unique()
        columns = dict(zip(bands, band_labels))
        subset = subset.rename(columns=columns)
        return pa.Table.from_pandas(subset)

    @staticmethod
    def cell_to_children_size(cell, desired_level: int) -> int:
        """
        Determine total number of children at some offset level.

        Implementation of interface function.
        """
        level = len(cell) // 2
        if desired_level < level:
            return 0
        return 100 ** (desired_level - level)

    @staticmethod
    def valid_set(cells: set) -> set[str]:
        """
        Implementation of interface function.
        """
        return set(filter(lambda c: not pd.isna(c) and len(c) >= 2, cells))

    def parent_cells(self, cells: set, resolution) -> map:
        """
        Implementation of interface function.
        """
        return map(lambda x: self.cell_to_parent(x, resolution), cells)

    def expected_count(self, parent: str, resolution: int):
        """
        Implementation of interface function.
        """
        return self.cell_to_children_size(parent, resolution)

    @staticmethod
    def cell_to_parent(cell: str, parent_level: int) -> str:
        """
        Returns cell parent at some offset level.

        Not a part of the RasterIndexer interface.
        """
        return cell[: parent_level * 2]

    @staticmethod
    def cell_to_point(cell: str) -> shapely.geometry.Point:
        return shapely.Point(mh.to_location(cell, center=True)[::-1])

    @staticmethod
    def cell_to_polygon(cell: str) -> shapely.geometry.Polygon:
        loc1, loc2, _ = mh.to_location_rect(cell)
        return shapely.Polygon(
            *[
                [
                    (loc1[1], loc1[0]),
                    (loc2[1], loc1[0]),
                    (loc2[1], loc2[0]),
                    (loc1[1], loc2[0]),
                    (loc1[1], loc1[0]),
                ]
            ]
        )
