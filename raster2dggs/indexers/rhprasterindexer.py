"""
@author: ndemaio
"""

import rhppandas  # Necessary import despite lack of explicit use

import rhealpixdggs.rhp_wrappers as rhpw
import pandas as pd
import shapely
from rhealpixdggs.dggs import WGS84_003

import raster2dggs.constants as const
from raster2dggs.indexers.rasterindexer import RasterIndexer


class RHPRasterIndexer(RasterIndexer):
    """
    Provides integration for MWLR's rHEALPix DGGS.
    """

    def _index_window(self, wide, resolution: int, parent_res: int):
        rhpdf = wide.rhp.geo_to_rhp(resolution, lat_col="y", lng_col="x").drop(
            columns=["x", "y"]
        )
        return rhpdf.rhp.rhp_to_parent(parent_res).reset_index()

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

    def cell_area_m2(self, resolution: int, lat: float, lon: float) -> float:
        # rHEALPix is equal-area: 6 face cells at resolution 1, each subdividing by 9.
        # At resolution n>=1: 6 * 9^(n-1) cells; resolution 0 is the single whole-globe cell.
        if resolution == 0:
            return const.WGS84_SURFACE_AREA_M2
        return const.WGS84_SURFACE_AREA_M2 / (6 * 9 ** (resolution - 1))

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
