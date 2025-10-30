"""
@author: ndemaio
"""

from numbers import Number
from typing import Tuple

import a5 as a5py
import pandas as pd
import pyarrow as pa
import xarray as xr
import numpy as np
import shapely

import raster2dggs.constants as const

from raster2dggs.indexers.rasterindexer import RasterIndexer


class A5RasterIndexer(RasterIndexer):
    """
    Provides integration for the A5 DGGS.
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
        Index a raster window to A5.
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
        cells = [
            a5py.lonlat_to_cell((lon, lat), resolution)
            for lon, lat in zip(subset["x"], subset["y"])
        ]  # NB a5py.lonlat_to_cell is quite slow
        # Secondary (parent) A5 index, used later for partitioning
        a5_parent = [a5py.cell_to_parent(cell, parent_res) for cell in cells]
        subset = subset.drop(columns=["x", "y"])
        index_col = self.index_col(resolution)
        subset[index_col] = pd.Series(map(a5py.u64_to_hex, cells), index=subset.index)
        partition_col = self.partition_col(parent_res)
        subset[partition_col] = pd.Series(
            map(a5py.u64_to_hex, a5_parent), index=subset.index
        )
        # Renaming columns to actual band labels
        bands = sdf["band"].unique()
        columns = dict(zip(bands, band_labels))
        subset = subset.rename(columns=columns)
        return pa.Table.from_pandas(subset)

    @staticmethod
    def cell_to_children_size(cell: int, desired_resolution: int) -> int:
        """
        Determine total number of children at some offset resolution

        Implementation of interface function.
        """
        cell_level = a5py.get_resolution(cell)
        return 4 ** (desired_resolution - cell_level)

    def valid_set(self, cells: set) -> set[str]:
        """
        Implementation of interface function.
        """
        return set(
            filter(
                lambda c: (not pd.isna(c))
                and self.is_valid_a5_cell(
                    c,
                ),
                cells,
            ),
        )

    @staticmethod
    def parent_cells(cells: set, resolution) -> map:
        """
        Implementation of interface function.
        """
        return map(
            a5py.u64_to_hex,
            map(
                lambda x: a5py.cell_to_parent(x, resolution),
                map(a5py.hex_to_u64, cells),
            ),
        )

    def expected_count(self, parent: str, resolution: int):
        """
        Implementation of interface function.
        """
        return self.cell_to_children_size(a5py.hex_to_u64(parent), resolution)

    @staticmethod
    def is_valid_a5_cell(
        cell: str,
    ) -> bool:
        """
        Returns cell validity.

        Not a part of the RasterIndexer interface
        """
        cell = a5py.hex_to_u64(cell)
        try:
            c: a5py.A5Cell = a5py.core.serialization.deserialize(cell)
        except TypeError:
            return False
        return True

    @staticmethod
    def cell_to_point(cell: str) -> shapely.geometry.Point:
        cell = a5py.hex_to_u64(cell)
        return shapely.Point(a5py.cell_to_lonlat(cell))

    @staticmethod
    def cell_to_polygon(cell: str) -> shapely.geometry.Polygon:
        cell = a5py.hex_to_u64(cell)
        return shapely.Polygon(tuple(a5py.cell_to_boundary(cell)))
