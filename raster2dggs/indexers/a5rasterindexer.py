"""
@author: ndemaio
"""

import functools
import threading

import a5 as a5py
import numpy as np
import pandas as pd
import shapely

import raster2dggs.constants as const
from raster2dggs.indexers.rasterindexer import RasterIndexer

# pya5 keeps unlocked module-level mutable state (e.g. a single-entry cache of
# the last resolved cell, used to speed up dense-sample loops) and is not
# thread-safe. raster2dggs calls into it from multiple threads (per-window in
# Stage 1, per-partition in Stage 2's dask map_partitions), so every entry
# point into a5py is serialised through this lock. Reentrant because some
# methods below call each other (e.g. valid_set -> is_valid_a5_cell).
_A5_LOCK = threading.RLock()


def _locked(fn):
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        with _A5_LOCK:
            return fn(*args, **kwargs)

    return wrapper


class A5RasterIndexer(RasterIndexer):
    """
    Provides integration for the A5 DGGS.
    """

    @_locked
    def _index_window(self, wide, resolution: int, parent_res: int):
        # pya5 has no batch lonlat_to_cell API, so this is a per-point loop
        # (~200us/point).
        cells = [
            a5py.lonlat_to_cell((lon, lat), resolution)
            for lon, lat in zip(wide["x"], wide["y"])
        ]
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
    @_locked
    def cell_to_children_size(cell: int, desired_resolution: int) -> int:
        """
        Determine total number of children at some offset resolution

        Implementation of interface function.
        """
        cell_level = a5py.get_resolution(cell)
        return a5py.get_num_children(cell_level, desired_resolution)

    @_locked
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
    @_locked
    def parent_cells(cells: set, resolution) -> map:
        """
        Implementation of interface function.
        """
        # Materialised eagerly (not a lazy map) so the a5py calls happen while
        # the lock is held, rather than later when the caller consumes it.
        return list(
            map(
                a5py.u64_to_hex,
                map(
                    lambda x: a5py.cell_to_parent(x, resolution),
                    map(a5py.hex_to_u64, cells),
                ),
            )
        )

    @_locked
    def expected_count(self, parent: str, resolution: int):
        """
        Implementation of interface function.
        """
        return self.cell_to_children_size(a5py.hex_to_u64(parent), resolution)

    @staticmethod
    @_locked
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
        Return A5 cell IDs (hex strings) at the given resolution whose centres
        fall within the WGS84 bounding box.

        Uses pya5's polygon_to_cells (dense boundary sampling + flood fill) to
        find every cell whose centre lies in the bbox, then uncompact to expand
        the (possibly-compacted) result to the target resolution.
        """
        ring = [
            (min_lon, min_lat),
            (max_lon, min_lat),
            (max_lon, max_lat),
            (min_lon, max_lat),
            (min_lon, min_lat),
        ]
        compacted = a5py.polygon_to_cells(ring, resolution)
        cells = a5py.uncompact(compacted, resolution)
        return {a5py.u64_to_hex(c) for c in cells}

    @_locked
    def cell_area_m2(self, resolution: int, lat: float, lon: float) -> float:
        # A5 is equal-area: every cell at a given resolution has the same area,
        # so lat/lon are unused. Subdivision is aperture 5 from resolution 0 to
        # 1 (12 pentagons -> 60 cells) and aperture 4 thereafter; pya5's
        # cell_area() accounts for this directly.
        return a5py.cell_area(resolution)

    @staticmethod
    @_locked
    def cells_to_lonlat_arrays(cells: pd.Series) -> tuple[np.ndarray, np.ndarray]:
        # a5py.cell_to_lonlat returns (lon, lat) directly
        pts = np.array([a5py.cell_to_lonlat(a5py.hex_to_u64(c)) for c in cells])
        # a5py may return longitudes outside [-180, 180]; normalise to standard range.
        lons = (pts[:, 0] + 180.0) % 360.0 - 180.0
        lats = pts[:, 1]
        return lons, lats

    @staticmethod
    @_locked
    def cell_to_point(cell: str) -> shapely.geometry.Point:
        cell_u64 = a5py.hex_to_u64(cell)
        lon, lat = a5py.cell_to_lonlat(cell_u64)
        lon = (lon + 180.0) % 360.0 - 180.0  # normalise to [-180, 180]
        return shapely.Point(lon, lat)

    @staticmethod
    @_locked
    def cell_to_polygon(cell: str) -> shapely.geometry.Polygon:
        cell = a5py.hex_to_u64(cell)
        return shapely.Polygon(tuple(a5py.cell_to_boundary(cell)))
