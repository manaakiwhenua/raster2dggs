import functools
import threading

import numpy as np
import pandas as pd
import rhealpixdggs.rhp_wrappers as rhpw
import shapely
from rhealpixdggs.dggs import WGS84_003

import raster2dggs.constants as const
from raster2dggs.indexers.rasterindexer import RasterIndexer

# WGS84_003 (the shared rhealpixdggs singleton used throughout this file) keeps
# an unlocked, lazily-populated cache of projection helpers
# (RHEALPixDGGS._projection_cache in rhealpixdggs/dggs.py), populated via a
# check-then-write pattern that isn't safe under concurrent access. raster2dggs
# calls into it from multiple threads (per-window in Stage 1, per-partition in
# Stage 2's dask map_partitions), so every entry point that touches
# rhpw/WGS84_003 is serialised through this lock. Methods that are pure suid
# string arithmetic (parents, children counts) never call the library and stay
# lock-free.
_RHP_LOCK = threading.RLock()


def _locked(fn):
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        with _RHP_LOCK:
            return fn(*args, **kwargs)

    return wrapper


def _parent_cell(cell: str, resolution: int) -> str:
    """Ancestor of ``cell`` at ``resolution`` by suid truncation: an rHP index
    is a base-cell letter plus one digit per level (len == resolution + 1), so
    the ancestor is a prefix. A cell at or above the requested resolution is
    returned unchanged."""
    return cell[: resolution + 1]


class RHPRasterIndexer(RasterIndexer):
    """
    Provides integration for MWLR's rHEALPix DGGS.

    Cell IDs are index strings (e.g. "Q3330"). rhealpixdggs's array entry
    points (cells_from_points, centroids, boundary_array) handle each window
    or cell batch in one library call.
    """

    @_locked
    def _index_window(self, wide, resolution: int, parent_res: int):
        cells = pd.Series(
            WGS84_003.cells_from_points(
                wide["x"].to_numpy(), wide["y"].to_numpy(), resolution, plane=False
            ),
            index=wide.index,
            dtype=object,
        )
        # cells_from_points marks a point no cell contains with ""; as None it
        # is visible to the shared pd.isna-based nodata filtering.
        cells = cells.replace("", None)
        wide = wide.drop(columns=["x", "y"])
        wide[self.index_col(resolution)] = cells
        # Parents are suid prefixes; .str propagates None.
        wide[self.partition_col(parent_res)] = cells.str.slice(0, parent_res + 1)
        return wide

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
        return set(filter(lambda c: not pd.isna(c) and c != "", cells))

    @staticmethod
    def parent_cells(cells: set, resolution) -> list:
        """
        Implementation of interface function.
        """
        # Pure suid truncation; no library call, so no lock needed.
        return [_parent_cell(c, resolution) for c in cells]

    def expected_count(self, parent: str, resolution: int):
        """
        Implementation of interface function.
        """
        return self.cell_to_children_size(parent, resolution)

    SUPPORTS_CELL_ENUMERATION: bool = True

    @_locked
    def cells_in_bbox(
        self,
        min_lon: float,
        min_lat: float,
        max_lon: float,
        max_lat: float,
        resolution: int,
    ) -> set:
        """
        Return rHEALPix cell IDs at the given resolution whose centres fall
        within the WGS84 bounding box.

        Uses rhealpixdggs's polyfill, which enumerates cells covering the
        bbox's bounding region (via cells_from_region) and filters to those
        whose centroid lies inside the geometry.
        """
        polygon = shapely.geometry.box(min_lon, min_lat, max_lon, max_lat)
        cells = rhpw.polyfill(polygon, resolution, plane=False, dggs=WGS84_003)
        return cells if cells is not None else set()

    def cell_area_m2(self, resolution: int, lat: float, lon: float) -> float:
        # rHEALPix is equal-area: 6 face cells at resolution 1, each subdividing by 9.
        # At resolution n>=1: 6 * 9^(n-1) cells; resolution 0 is the single whole-globe cell.
        if resolution == 0:
            return const.WGS84_SURFACE_AREA_M2
        return const.WGS84_SURFACE_AREA_M2 / (6 * 9 ** (resolution - 1))

    @staticmethod
    @_locked
    def cells_to_lonlat_arrays(cells: pd.Series) -> tuple[np.ndarray, np.ndarray]:
        # A cell's representative point is its centroid, not its nucleus (the
        # two differ for the dart and skew cells). One call returns (n, 2) lon/lat.
        arr = WGS84_003.centroids(list(cells), plane=False)
        return arr[:, 0], arr[:, 1]

    @staticmethod
    @_locked
    def cell_to_point(cell: str) -> shapely.geometry.Point:
        # Scalar call: this interface is per-cell, and a one-cell centroids()
        # pays array overhead exceeding the scalar cost.
        return shapely.Point(rhpw.rhp_to_geo(cell, plane=False, dggs=WGS84_003))

    @_locked
    def cells_to_points(self, cells) -> np.ndarray:
        return shapely.points(WGS84_003.centroids(list(cells), plane=False))

    @_locked
    def cells_to_polygons(self, cells) -> np.ndarray:
        # (n, 4, 2) vertex array -> n Polygons in one vectorised call.
        return shapely.polygons(WGS84_003.boundary_array(list(cells), n=2, plane=False))

    @staticmethod
    @_locked
    def cell_to_polygon(cell: str) -> shapely.geometry.Polygon:
        # Scalar call: this interface is per-cell, and a one-cell
        # boundary_array pays array overhead several times the scalar cost.
        return shapely.Polygon(
            rhpw.rhp_to_geo_boundary(cell, plane=False, dggs=WGS84_003)
        )
