"""
@author: ndemaio
"""

import h3 as h3py
import pandas as pd
import shapely

from raster2dggs.indexers.rasterindexer import RasterIndexer


class H3RasterIndexer(RasterIndexer):
    """
    Provides integration for Uber's H3 DGGS.
    """

    def _index_window(self, wide, resolution: int, parent_res: int):
        index_col = self.index_col(resolution)
        partition_col = self.partition_col(parent_res)
        cells = [
            h3py.latlng_to_cell(lat, lon, resolution)
            for lat, lon in zip(wide["y"], wide["x"])
        ]
        wide = wide.drop(columns=["x", "y"])
        wide[index_col] = cells
        wide[partition_col] = [h3py.cell_to_parent(c, parent_res) for c in cells]
        return wide.reset_index(drop=True)

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

    def cell_area_m2(self, resolution: int, lat: float, lon: float) -> float:
        cell = h3py.latlng_to_cell(lat, lon, resolution)
        return h3py.cell_area(cell, unit="m^2")

    @staticmethod
    def cell_to_point(cell: str) -> shapely.geometry.Point:
        return shapely.Point(h3py.cell_to_latlng(cell)[::-1])

    @staticmethod
    def cell_to_polygon(cell: str) -> shapely.geometry.Polygon:
        return shapely.Polygon(
            tuple(coord[::-1] for coord in h3py.cell_to_boundary(cell))
        )
