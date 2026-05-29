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
            return (
                const.MIN_A5
                <= a5py.get_resolution(a5py.hex_to_u64(cell))
                <= const.MAX_A5
            )
        except Exception:
            return False

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
        Return A5 cell IDs (hex strings) at the given resolution whose centres
        fall within the WGS84 bounding box.

        Uses a top-down recursive descent from the 12 res-0 cells, pruning
        branches where the cell centroid cannot possibly be inside the bbox
        at the target resolution.

        NOTE: the upstream A5 API exposes a native ``polygonToCells`` function
        (https://a5geo.org/docs/api-reference/regions#polygontocells) that
        returns compacted cells whose centres lie inside a polygon ring.  This
        would be the preferred implementation, but ``a5-fast`` v0.2 (the
        Python binding used here) does not yet include the region-covering
        functions.  Replace this implementation with ``polygonToCells`` +
        ``uncompact`` once a version of ``a5-fast`` that exposes it is
        released.
        """
        result = set()

        def _cell_radius_deg(cell_res: int) -> float:
            """Approximate half-width of an A5 cell in degrees."""
            # A5: 12 * 4^res cells
            n = 12 * (4**cell_res)
            return (const.EARTH_DEGREE_SQUARE / n) ** 0.5 * 0.75

        def _recurse(cell_u64: int, cell_res: int):
            _lon, lat = a5py.cell_to_lonlat(cell_u64)
            # a5py may return longitudes outside [-180, 180]; normalise.
            lon = (_lon + 180.0) % 360.0 - 180.0
            rad = _cell_radius_deg(cell_res)
            if lat < min_lat - rad or lat > max_lat + rad:
                return
            # Antimeridian-safe lon test: check raw and ±360-shifted positions.
            lon_in_range = (
                (min_lon - rad <= lon <= max_lon + rad)
                or (min_lon - rad <= lon + 360 <= max_lon + rad)
                or (min_lon - rad <= lon - 360 <= max_lon + rad)
            )
            if not lon_in_range:
                return

            if cell_res == resolution:
                if min_lat <= lat <= max_lat and min_lon <= lon <= max_lon:
                    result.add(a5py.u64_to_hex(cell_u64))
                return

            for child in a5py.cell_to_children(cell_u64, cell_res + 1):
                _recurse(child, cell_res + 1)

        for c in a5py.get_res0_cells():
            _recurse(c, 0)

        return result

    def cell_area_m2(self, resolution: int, lat: float, lon: float) -> float:
        # A5 is equal-area: 12 cells at resolution 0, each subdividing by 4
        return const.WGS84_SURFACE_AREA_M2 / (12 * 4**resolution)

    @staticmethod
    def cells_to_lonlat_arrays(cells: pd.Series) -> tuple[np.ndarray, np.ndarray]:
        # a5py.cell_to_lonlat returns (lon, lat) directly
        pts = np.array([a5py.cell_to_lonlat(a5py.hex_to_u64(c)) for c in cells])
        # a5py may return longitudes outside [-180, 180]; normalise to standard range.
        lons = (pts[:, 0] + 180.0) % 360.0 - 180.0
        lats = pts[:, 1]
        return lons, lats

    @staticmethod
    def cell_to_point(cell: str) -> shapely.geometry.Point:
        cell_u64 = a5py.hex_to_u64(cell)
        lon, lat = a5py.cell_to_lonlat(cell_u64)
        lon = (lon + 180.0) % 360.0 - 180.0  # normalise to [-180, 180]
        return shapely.Point(lon, lat)

    @staticmethod
    def cell_to_polygon(cell: str) -> shapely.geometry.Polygon:
        cell = a5py.hex_to_u64(cell)
        return shapely.Polygon(tuple(a5py.cell_to_boundary(cell)))
