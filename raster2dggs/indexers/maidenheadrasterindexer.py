import maidenhead as mh
import numpy as np
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
            mh.to_maiden(lat, lon, resolution) for lat, lon in zip(wide["y"], wide["x"])
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

    # Maidenhead grid step sizes (lon, lat) per resolution level.
    # Level 1: 20°×10° field. Each subsequent pair of characters subdivides
    # by 10 (squares) then 24 (sub-squares), alternating.
    # Level 1: 20°×10°, Level 2: 2°×1°, Level 3: 5'/12 × 2.5'/12 = 0.0833°×0.0417°
    # Exact values: lon_step = 20/10^(level-1) / (24 if level even else 1) ...
    # Using the known pattern: step halves each level pair.
    _LON_STEPS = {
        1: 20.0,
        2: 2.0,
        3: 2.0 / 24,
        4: 2.0 / 240,
        5: 2.0 / 5760,
        6: 2.0 / 57600,
    }
    _LAT_STEPS = {
        1: 10.0,
        2: 1.0,
        3: 1.0 / 24,
        4: 1.0 / 240,
        5: 1.0 / 5760,
        6: 1.0 / 57600,
    }

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
        Return all Maidenhead locator cells at the given level whose centres
        fall inside the WGS84 bounding box.

        The Maidenhead grid is regular in lon/lat space with fixed step sizes,
        so cells can be enumerated analytically from the SW corner.
        """
        lon_step = self._LON_STEPS[resolution]
        lat_step = self._LAT_STEPS[resolution]

        # Encode SW corner to get an aligned starting cell, then iterate.
        sw = mh.to_maiden(min_lat, min_lon, resolution)
        sw_lat, sw_lon = mh.to_location(sw, center=True)

        result = set()
        i_lat = 0
        while (lat := sw_lat + i_lat * lat_step) <= max_lat:
            i_lon = 0
            while (lon := sw_lon + i_lon * lon_step) <= max_lon:
                if min_lat <= lat <= max_lat and min_lon <= lon <= max_lon:
                    result.add(mh.to_maiden(lat, lon, resolution))
                i_lon += 1
            i_lat += 1
        return result

    def cell_area_m2(self, resolution: int, lat: float, lon: float) -> float:
        cell = mh.to_maiden(lat, lon, resolution)
        loc1, loc2, _ = mh.to_location_rect(cell)
        bbox = shapely.geometry.box(
            min(loc1[1], loc2[1]),
            min(loc1[0], loc2[0]),
            max(loc1[1], loc2[1]),
            max(loc1[0], loc2[0]),
        )
        area_m2, _ = pyproj.Geod(ellps="WGS84").geometry_area_perimeter(bbox)
        return abs(area_m2)

    @staticmethod
    def cells_to_lonlat_arrays(cells: pd.Series) -> tuple[np.ndarray, np.ndarray]:
        pts = np.array([mh.to_location(c, center=True) for c in cells])
        return pts[:, 1], pts[:, 0]  # lons, lats (to_location returns lat, lon)

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
