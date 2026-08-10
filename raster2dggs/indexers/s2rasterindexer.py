import math
from math import ceil

import numpy as np
import pandas as pd
import pyarrow as pa
import s2sphere
import shapely

import raster2dggs.constants as const
from raster2dggs.indexers.rasterindexer import RasterIndexer


class S2RasterIndexer(RasterIndexer):
    """
    Provides integration for Google's S2 DGGS.

    Cells are handled as uint64 CellId values throughout; tokens are produced
    only at the output boundary via cells_to_string. Note tokens are not plain
    hex of the id -- trailing zeros are stripped -- and ids on faces 4 and 5
    exceed int64, which is why the working type is unsigned.
    """

    CELL_ARROW_TYPE: pa.DataType = pa.uint64()

    def cells_to_string(self, cells) -> list:
        return [s2sphere.CellId(int(c)).to_token() for c in cells]

    def _index_window(self, wide, resolution: int, parent_res: int):
        cells = [
            s2sphere.CellId.from_lat_lng(s2sphere.LatLng.from_degrees(lat, lon))
            for lat, lon in zip(wide["y"], wide["x"], strict=True)
        ]
        n = len(cells)
        wide = wide.drop(columns=["x", "y"])
        wide[self.index_col(resolution)] = np.fromiter(
            (c.parent(resolution).id() for c in cells), dtype=np.uint64, count=n
        )
        wide[self.partition_col(parent_res)] = np.fromiter(
            (c.parent(parent_res).id() for c in cells), dtype=np.uint64, count=n
        )
        return wide

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
    def valid_set(cells: set) -> set:
        """
        Implementation of interface function.
        """
        return set(
            filter(
                lambda c: (not pd.isna(c)) and s2sphere.CellId(int(c)).is_valid(),
                cells,
            )
        )

    @staticmethod
    def parent_cells(cells: set, resolution) -> map:
        """
        Implementation of interface function.
        """
        return map(
            lambda c: s2sphere.CellId(int(c)).parent(resolution).id(),
            cells,
        )

    def expected_count(self, parent, resolution: int):
        """
        Implementation of interface function.
        """
        return self.cell_to_children_size(s2sphere.CellId(int(parent)), resolution)

    SUPPORTS_CELL_ENUMERATION: bool = True

    def cells_in_bbox(
        self,
        min_lon: float,
        min_lat: float,
        max_lon: float,
        max_lat: float,
        resolution: int,
    ) -> set:
        """
        Return S2 cell tokens at the given level whose centres fall within the
        WGS84 bounding box.

        Uses S2's RegionCoverer with max_cells computed from the ratio of the
        bbox area to the S2 cell area at the target level (following the same
        approach as vector2dggs). The covering is then filtered to cells whose
        centre is actually inside the bbox.
        """
        # Estimate bbox area in m² using a flat-earth approximation (good enough
        # for a max_cells upper bound).
        lat_c = math.radians((min_lat + max_lat) / 2)
        bbox_area_m2 = (
            (max_lat - min_lat)
            * const.WGS84_APPROX_DISTANCE_DEG_M
            * (max_lon - min_lon)
            * const.WGS84_APPROX_DISTANCE_DEG_M
            * math.cos(lat_c)
        )
        cell_area_m2 = const.WGS84_SURFACE_AREA_M2 / (6 * 4**resolution)
        max_cells = ceil(max(64, bbox_area_m2 / cell_area_m2) * 1.1)

        r = s2sphere.RegionCoverer()
        r.min_level = resolution
        r.max_level = resolution
        r.max_cells = max_cells
        ll_rect = s2sphere.LatLngRect(
            s2sphere.LatLng.from_degrees(min_lat, min_lon),
            s2sphere.LatLng.from_degrees(max_lat, max_lon),
        )
        covering = r.get_covering(ll_rect)
        result = set()
        for cell_id in covering:
            ll = s2sphere.LatLng.from_point(cell_id.to_point())
            lat = ll.lat().degrees
            lon = ll.lng().degrees
            if min_lat <= lat <= max_lat and min_lon <= lon <= max_lon:
                result.add(cell_id.id())
        return result

    def cell_area_m2(self, resolution: int, lat: float, lon: float) -> float:
        cell_id = s2sphere.CellId.from_lat_lng(
            s2sphere.LatLng.from_degrees(lat, lon)
        ).parent(resolution)
        # approx_area() returns steradians; multiply by Earth's mean radius squared
        return s2sphere.Cell(cell_id).approx_area() * (const.EARTH_MEAN_RADIUS_M**2)

    @staticmethod
    def cells_to_lonlat_arrays(cells: pd.Series) -> tuple[np.ndarray, np.ndarray]:
        pts = np.array(
            [
                (ll.lng().degrees, ll.lat().degrees)
                for ll in (
                    s2sphere.LatLng.from_point(s2sphere.CellId(int(c)).to_point())
                    for c in cells
                )
            ]
        )
        return pts[:, 0], pts[:, 1]  # lons, lats

    @staticmethod
    def cell_to_point(cell) -> shapely.geometry.Point:
        latLng = s2sphere.LatLng.from_point(s2sphere.CellId(int(cell)).to_point())
        return shapely.Point(latLng.lng().degrees, latLng.lat().degrees)

    @staticmethod
    def cell_to_polygon(cell) -> shapely.geometry.Polygon:
        cell_id = s2sphere.CellId(int(cell))
        cell = s2sphere.Cell(cell_id)
        vertices = []
        for i in range(4):
            vertex = cell.get_vertex(i)
            lat_lng = s2sphere.LatLng.from_point(vertex)
            vertices.append((lat_lng.lng().degrees, lat_lng.lat().degrees))
        return shapely.Polygon(vertices)
