"""
@author: ndemaio
"""

import a5_fast as a5py
import numpy as np
import pandas as pd
import shapely

import raster2dggs.constants as const
from raster2dggs.indexers.rasterindexer import RasterIndexer


class A5RasterIndexer(RasterIndexer):
    """
    Provides integration for the A5 DGGS.
    """

    def _index_window(self, wide, resolution: int, parent_res: int):
        flat_coords = np.column_stack([wide["x"], wide["y"]]).ravel().tolist()
        cells = a5py.lonlat_to_cell_batch(flat_coords, resolution)
        a5_parent = [a5py.cell_to_parent(cell, parent_res) for cell in cells]
        wide = wide.drop(columns=["x", "y"])
        wide[self.index_col(resolution)] = pd.Series(
            map(a5py.u64_to_hex, cells), index=wide.index
        )
        wide[self.partition_col(parent_res)] = pd.Series(
            map(a5py.u64_to_hex, a5_parent), index=wide.index
        )
        return wide

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
    def is_valid_a5_cell(cell: str) -> bool:
        """
        Returns cell validity.

        Not a part of the RasterIndexer interface
        """
        try:
            return const.MIN_A5 <= a5py.get_resolution(a5py.hex_to_u64(cell)) <= const.MAX_A5
        except Exception:
            return False

    def cell_area_m2(self, resolution: int, lat: float, lon: float) -> float:
        # A5 is equal-area: 12 cells at resolution 0, each subdividing by 4
        return const.WGS84_SURFACE_AREA_M2 / (12 * 4 ** resolution)

    @staticmethod
    def cell_to_point(cell: str) -> shapely.geometry.Point:
        cell = a5py.hex_to_u64(cell)
        return shapely.Point(a5py.cell_to_lonlat(cell))

    @staticmethod
    def cell_to_polygon(cell: str) -> shapely.geometry.Polygon:
        cell = a5py.hex_to_u64(cell)
        return shapely.Polygon(tuple(a5py.cell_to_boundary(cell)))
