import geohash as gh
import numpy as np
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

    SUPPORTS_CELL_ENUMERATION: bool = True

    @staticmethod
    def _step_sizes(precision: int) -> tuple[float, float]:
        """
        Return (lat_step, lon_step) in degrees for a given geohash precision.

        Geohash encodes 5 bits per character. The bits alternate lon/lat
        starting with lon (MSB). For precision p:
          total bits = 5*p
          lon_bits   = ceil(5*p / 2)
          lat_bits   = floor(5*p / 2)
          lon_step   = 360 / 2^lon_bits
          lat_step   = 180 / 2^lat_bits
        """
        bits = precision * 5
        lon_bits = (bits + 1) // 2
        lat_bits = bits // 2
        return 180.0 / (2**lat_bits), 360.0 / (2**lon_bits)

    def cells_in_bbox(
        self,
        min_lon: float,
        min_lat: float,
        max_lon: float,
        max_lat: float,
        resolution: int,
    ) -> set:
        """
        Return all geohash cells at the given precision whose centres fall
        inside the WGS84 bounding box.

        Geohash cells form a regular grid in lon/lat space with fixed step
        sizes per precision, so we can enumerate them analytically.
        """
        lat_step, lon_step = self._step_sizes(resolution)

        # Align to the first cell centre at or just south-west of the bbox.
        # Cell centres sit at -90 + lat_step/2 + k*lat_step (similarly for lon).
        # Encode SW corner to get an anchor, then derive the grid from there.
        sw_hash = gh.encode(min_lat, min_lon, resolution)
        sw_lat, sw_lon, _, _ = gh.decode_exactly(sw_hash)

        result = set()
        i_lat = 0
        while (lat := sw_lat + i_lat * lat_step) <= max_lat:
            i_lon = 0
            while (lon := sw_lon + i_lon * lon_step) <= max_lon:
                if min_lat <= lat <= max_lat and min_lon <= lon <= max_lon:
                    result.add(gh.encode(lat, lon, resolution))
                i_lon += 1
            i_lat += 1
        return result

    def cell_area_m2(self, resolution: int, lat: float, lon: float) -> float:
        cell = gh.encode(lat, lon, precision=resolution)
        lat_c, lon_c, lat_err, lon_err = gh.decode_exactly(cell)
        bbox = shapely.geometry.box(
            lon_c - lon_err, lat_c - lat_err, lon_c + lon_err, lat_c + lat_err
        )
        area_m2, _ = pyproj.Geod(ellps="WGS84").geometry_area_perimeter(bbox)
        return abs(area_m2)

    @staticmethod
    def cells_to_lonlat_arrays(cells: pd.Series) -> tuple[np.ndarray, np.ndarray]:
        # decode_exactly returns (lat, lon, lat_err, lon_err)
        arr = np.array([(d[1], d[0]) for d in (gh.decode_exactly(c) for c in cells)])
        return arr[:, 0], arr[:, 1]

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
