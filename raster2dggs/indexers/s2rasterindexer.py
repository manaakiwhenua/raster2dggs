"""
@author: ndemaio
"""

import s2sphere
import pandas as pd
import shapely

from raster2dggs.indexers.rasterindexer import RasterIndexer


class S2RasterIndexer(RasterIndexer):
    """
    Provides integration for Google's S2 DGGS.
    """

    def _index_window(self, wide, resolution: int, parent_res: int):
        cells = [
            s2sphere.CellId.from_lat_lng(s2sphere.LatLng.from_degrees(lat, lon))
            for lat, lon in zip(wide["y"], wide["x"])
        ]
        wide = wide.drop(columns=["x", "y"])
        wide[self.index_col(resolution)] = pd.Series(
            [c.parent(resolution).to_token() for c in cells], index=wide.index
        )
        wide[self.partition_col(parent_res)] = pd.Series(
            [c.parent(parent_res).to_token() for c in cells], index=wide.index
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
    def valid_set(cells: set) -> set[str]:
        """
        Implementation of interface function.
        """
        return set(
            map(
                lambda c: c.to_token(),
                filter(
                    lambda c: c.is_valid(),
                    map(
                        lambda c: s2sphere.CellId.from_token(c),
                        filter(lambda c: not pd.isna(c), cells),
                    ),
                ),
            )
        )

    @staticmethod
    def parent_cells(cells: set, resolution) -> map:
        """
        Implementation of interface function.
        """
        return map(
            lambda token: s2sphere.CellId.from_token(token)
            .parent(resolution)
            .to_token(),
            cells,
        )

    def expected_count(self, parent: str, resolution: int):
        """
        Implementation of interface function.
        """
        return self.cell_to_children_size(
            s2sphere.CellId.from_token(parent), resolution
        )

    def cell_area_m2(self, resolution: int, lat: float, lon: float) -> float:
        cell_id = s2sphere.CellId.from_lat_lng(
            s2sphere.LatLng.from_degrees(lat, lon)
        ).parent(resolution)
        # approx_area() returns steradians; multiply by Earth's mean radius squared
        return s2sphere.Cell(cell_id).approx_area() * (6_371_000.0 ** 2)

    @staticmethod
    def cell_to_point(cell: str) -> shapely.geometry.Point:
        latLng = s2sphere.LatLng.from_point(s2sphere.CellId.from_token(cell).to_point())
        return shapely.Point(latLng.lng().degrees, latLng.lat().degrees)

    @staticmethod
    def cell_to_polygon(cell: str) -> shapely.geometry.Polygon:
        cell_id = s2sphere.CellId.from_token(cell)
        cell = s2sphere.Cell(cell_id)
        vertices = []
        for i in range(4):
            vertex = cell.get_vertex(i)
            lat_lng = s2sphere.LatLng.from_point(vertex)
            vertices.append((lat_lng.lng().degrees, lat_lng.lat().degrees))
        return shapely.Polygon(vertices)
