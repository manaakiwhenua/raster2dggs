import logging

import rhppandas  # Necessary import despite lack of explicit use

import numpy as np
import rhealpixdggs.rhp_wrappers as rhpw
import pandas as pd
import shapely
from rhealpixdggs.dggs import WGS84_003

import raster2dggs.constants as const
from raster2dggs.indexers.rasterindexer import RasterIndexer

LOGGER = logging.getLogger(__name__)


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
        Return rHEALPix cell IDs at the given resolution whose centres fall
        within the WGS84 bounding box.

        Primary path: CellZoneFromPoly, which is efficient for most regions.

        Fallback path: top-down recursive nucleus search from the 6 res-1 face
        cells. Used when CellZoneFromPoly raises an AttributeError, which
        happens for bounding boxes whose rHEALPix containing-cell has vertices
        on the ±180° antimeridian (e.g. New Zealand, Pacific Ocean).  The
        recursive approach avoids Shapely polygon operations entirely and is
        robust to antimeridian and polar-cap coordinate edge cases.
        """
        import shapely
        from rhealpixdggs.conversion import CellZoneFromPoly

        polygon = shapely.geometry.box(min_lon, min_lat, max_lon, max_lat)
        try:
            zone = CellZoneFromPoly([None, polygon], resolution, True)
            return {str(c) for c in zone.cells_list}
        except AttributeError:
            # CellZoneFromPoly raises AttributeError when get_finest_containing_cell
            # returns None — known to happen for bboxes whose containing cell has
            # vertices on the ±180° antimeridian (e.g. New Zealand, Pacific Ocean).
            # Fall through to the slower recursive fallback.
            LOGGER.debug(
                "CellZoneFromPoly raised AttributeError for bbox "
                "(%.4f, %.4f, %.4f, %.4f); using recursive fallback.",
                min_lon,
                min_lat,
                max_lon,
                max_lat,
            )

        # Recursive fallback
        result = set()

        def _cell_radius_deg(cell_res: int) -> float:
            """Approximate half-width in degrees of a cell at the given resolution."""
            if cell_res <= 0:
                return 180.0
            n_cells = 6 * (9 ** (cell_res - 1))
            return (const.EARTH_DEGREE_SQUARE / n_cells) ** 0.5 * 0.75

        def _recurse(cell, cell_res: int):
            lon, lat = cell.nucleus(plane=False)
            rad = _cell_radius_deg(cell_res)
            if lat < min_lat - rad or lat > max_lat + rad:
                return
            # Antimeridian-safe lon test: check both raw and ±360-shifted.
            lon_in_range = (
                (min_lon - rad <= lon <= max_lon + rad)
                or (min_lon - rad <= lon + 360 <= max_lon + rad)
                or (min_lon - rad <= lon - 360 <= max_lon + rad)
            )
            if not lon_in_range:
                return
            if cell_res == resolution:
                if min_lat <= lat <= max_lat and min_lon <= lon <= max_lon:
                    result.add(str(cell))
                return
            for child in cell.subcells():
                _recurse(child, cell_res + 1)

        for face in WGS84_003.grid(1):
            _recurse(face, 1)

        return result

    def cell_area_m2(self, resolution: int, lat: float, lon: float) -> float:
        # rHEALPix is equal-area: 6 face cells at resolution 1, each subdividing by 9.
        # At resolution n>=1: 6 * 9^(n-1) cells; resolution 0 is the single whole-globe cell.
        if resolution == 0:
            return const.WGS84_SURFACE_AREA_M2
        return const.WGS84_SURFACE_AREA_M2 / (6 * 9 ** (resolution - 1))

    @staticmethod
    def cells_to_lonlat_arrays(cells: pd.Series) -> tuple[np.ndarray, np.ndarray]:
        # rhp_to_geo returns (lon, lat) as numpy floats already
        arr = np.array([rhpw.rhp_to_geo(c, plane=False, dggs=WGS84_003) for c in cells])
        return arr[:, 0], arr[:, 1]

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
