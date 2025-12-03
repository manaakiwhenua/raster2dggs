"""
@author: ndemaio
"""

from numbers import Number
from typing import Tuple

import s2sphere
import pandas as pd
import pyarrow as pa
import xarray as xr
import numpy as np
import shapely

import raster2dggs.constants as const

from raster2dggs.indexers.rasterindexer import RasterIndexer


class S2RasterIndexer(RasterIndexer):
    """
    Provides integration for Google's S2 DGGS.
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
        Index a raster window to S2.
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
        # S2 index
        cells = [
            s2sphere.CellId.from_lat_lng(s2sphere.LatLng.from_degrees(lat, lon))
            for lat, lon in zip(subset["y"], subset["x"])
        ]
        s2 = [cell.parent(resolution).to_token() for cell in cells]
        s2_parent = [cell.parent(parent_res).to_token() for cell in cells]
        subset = subset.drop(columns=["x", "y"])
        index_col = self.index_col(resolution)
        subset[index_col] = pd.Series(s2, index=subset.index)
        partition_col = self.partition_col(parent_res)
        subset[partition_col] = pd.Series(s2_parent, index=subset.index)
        # Renaming columns to actual band labels
        bands = sdf["band"].unique()
        columns = dict(zip(bands, band_labels))
        subset = subset.rename(columns=columns)
        return pa.Table.from_pandas(subset)

    @staticmethod
    def cell_to_children_size(cell, desired_resolution: int) -> int:
        """
        Determine total number of children at some offset resolution

        Implementation of interface function.
        """
        # return sum(1 for _ in cell.children(desired_resolution)) # Expensive eumeration
        cell_level = cell.level()
        if cell_level == 0:
            # At level 0, there are 6 initial cells on the S2 sphere.
            # Each of these divides into 4^n children at any subsequent level n.
            return 6 * (4 ** (desired_resolution - 1))
        # For levels greater than 0, the cell divides into 4^(n-m) children
        return 4 ** (desired_resolution - cell_level)

    @staticmethod
    def valid_set(cells: set) -> set[str]:
        """
        Implementation of interface function.
        """
        return set(
            map(
                lambda c: c.to_token(),
                filter(
                    lambda c: c.is_valid(),
                    map(
                        lambda c: s2sphere.CellId.from_token(c),
                        filter(lambda c: not pd.isna(c), cells),
                    ),
                ),
            )
        )

    @staticmethod
    def parent_cells(cells: set, resolution) -> map:
        """
        Implementation of interface function.
        """
        return map(
            lambda token: s2sphere.CellId.from_token(token)
            .parent(resolution)
            .to_token(),
            cells,
        )

    def expected_count(self, parent: str, resolution: int):
        """
        Implementation of interface function.
        """
        return self.cell_to_children_size(
            s2sphere.CellId.from_token(parent), resolution
        )

    @staticmethod
    def cell_to_point(cell: str) -> shapely.geometry.Point:
        latLng = s2sphere.LatLng.from_point(s2sphere.CellId.from_token(cell).to_point())
        return shapely.Point(latLng.lng().degrees, latLng.lat().degrees)

    @staticmethod
    def cell_to_polygon(cell: str) -> shapely.geometry.Polygon:
        cell_id = s2sphere.CellId.from_token(cell)
        cell = s2sphere.Cell(cell_id)
        vertices = []
        for i in range(4):
            vertex = cell.get_vertex(i)
            lat_lng = s2sphere.LatLng.from_point(vertex)
            vertices.append((lat_lng.lng().degrees, lat_lng.lat().degrees))
        return shapely.Polygon(vertices)
