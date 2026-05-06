"""
@author: ndemaio
"""

import maidenhead as mh
import pandas as pd
import pyproj
import shapely

from raster2dggs.indexers.rasterindexer import RasterIndexer


class MaidenheadRasterIndexer(RasterIndexer):
    """
    Provides integration for Maidenhead Locator System geocodes.
    """

    def _index_window(self, wide, resolution: int, parent_res: int):
        maidenhead = [
            mh.to_maiden(lat, lon, resolution)
            for lat, lon in zip(wide["y"], wide["x"])
        ]
        maidenhead_parent = [self.cell_to_parent(m, parent_res) for m in maidenhead]
        wide = wide.drop(columns=["x", "y"])
        wide[self.index_col(resolution)] = pd.Series(maidenhead, index=wide.index)
        wide[self.partition_col(parent_res)] = pd.Series(
            maidenhead_parent, index=wide.index
        )
        return wide

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

    def cell_area_m2(self, resolution: int, lat: float, lon: float) -> float:
        cell = mh.to_maiden(lat, lon, resolution)
        loc1, loc2, _ = mh.to_location_rect(cell)
        bbox = shapely.geometry.box(
            min(loc1[1], loc2[1]), min(loc1[0], loc2[0]),
            max(loc1[1], loc2[1]), max(loc1[0], loc2[0]),
        )
        area_m2, _ = pyproj.Geod(ellps="WGS84").geometry_area_perimeter(bbox)
        return abs(area_m2)

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
