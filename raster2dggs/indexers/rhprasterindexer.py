import functools
import threading

import rhppandas  # Necessary import despite lack of explicit use

import numpy as np
import rhealpixdggs.rhp_wrappers as rhpw
import pandas as pd
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
# rhpw/WGS84_003 is serialised through this lock.
_RHP_LOCK = threading.RLock()


def _locked(fn):
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        with _RHP_LOCK:
            return fn(*args, **kwargs)

    return wrapper


class RHPRasterIndexer(RasterIndexer):
    """
    Provides integration for MWLR's rHEALPix DGGS.
    """

    @_locked
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
    @_locked
    def parent_cells(cells: set, resolution) -> map:
        """
        Implementation of interface function.
        """
        # Materialised eagerly (not a lazy map) so the rhpw calls happen while
        # the lock is held, rather than later when the caller consumes it.
        return [rhpw.rhp_to_parent(x, resolution) for x in cells]

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
        cells = rhpw.polyfill(polygon, resolution, plane=False)
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
        # rhp_to_geo returns (lon, lat) as numpy floats already
        arr = np.array([rhpw.rhp_to_geo(c, plane=False, dggs=WGS84_003) for c in cells])
        return arr[:, 0], arr[:, 1]

    @staticmethod
    @_locked
    def cell_to_point(cell: str) -> shapely.geometry.Point:
        return shapely.Point(rhpw.rhp_to_geo(cell, plane=False, dggs=WGS84_003))

    @staticmethod
    @_locked
    def cell_to_polygon(cell: str) -> shapely.geometry.Polygon:
        return shapely.Polygon(
            tuple(
                coord
                for coord in rhpw.rhp_to_geo_boundary(cell, plane=False, dggs=WGS84_003)
            )
        )
