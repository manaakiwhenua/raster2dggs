"""
@author: ndemaio
"""

import geohash as gh
import pandas as pd
import pyproj
import shapely

from raster2dggs.indexers.rasterindexer import RasterIndexer


class GeohashRasterIndexer(RasterIndexer):
    """
    Provides integration for the Geohash geocode system.
    """

    def _index_window(self, wide, resolution: int, parent_res: int):
        geohash = [
            gh.encode(lat, lon, precision=resolution)
            for lat, lon in zip(wide["y"], wide["x"])
        ]
        wide = wide.drop(columns=["x", "y"])
        wide[self.index_col(resolution)] = pd.Series(geohash, index=wide.index)
        wide[self.partition_col(parent_res)] = pd.Series(
            [g[:parent_res] for g in geohash], index=wide.index
        )
        return wide

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

    def cell_area_m2(self, resolution: int, lat: float, lon: float) -> float:
        cell = gh.encode(lat, lon, precision=resolution)
        lat_c, lon_c, lat_err, lon_err = gh.decode_exactly(cell)
        bbox = shapely.geometry.box(
            lon_c - lon_err, lat_c - lat_err, lon_c + lon_err, lat_c + lat_err
        )
        area_m2, _ = pyproj.Geod(ellps="WGS84").geometry_area_perimeter(bbox)
        return abs(area_m2)

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
