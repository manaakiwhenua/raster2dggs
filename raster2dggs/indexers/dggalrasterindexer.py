"""
@author: ndemaio
"""

from abc import abstractmethod
from functools import reduce
from numbers import Number
from typing import Tuple

import h3pandas  # Necessary import despite lack of explicit use

import dggal
import pandas as pd
import pyarrow as pa
import xarray as xr
import numpy as np
import shapely

import raster2dggs.constants as const

from raster2dggs.indexers.rasterindexer import RasterIndexer


# TODO memoisation; needs to use hashable inputs and outputs (int)
def apply_n_reduce(f, n, x):
    def step(v):
        return f(v)[0]

    return reduce(lambda v, _: step(v), range(n), x)


class DGGALRasterIndexer(RasterIndexer):
    """
    Provides integration for Uber's H3 DGGS.
    """

    @property
    @abstractmethod
    def dggrs(self) -> dggal.DGGRS:
        raise NotImplementedError

    @property
    @abstractmethod
    def refinementRatio(self) -> int:
        raise NotImplementedError

    def index_func(
        self,
        sdf: xr.DataArray,
        resolution: int,
        parent_res: int,
        nodata: Number = np.nan,
        band_labels: Tuple[str] = None,
    ) -> pa.Table:
        """
        Index a raster window to a DGGRS.
        Subsequent steps are necessary to resolve issues at the boundaries of windows.
        If windows are very small, or in strips rather than blocks, processing may be slower
        than necessary and the recommendation is to write different windows in the source raster.

        Implementation of interface function.
        """
        sdf: pd.DataFrame = (
            sdf.to_dataframe().drop(columns=["spatial_ref"]).reset_index()
        )
        subset: pd.DataFrame = sdf.dropna()
        subset = subset[subset.value != nodata]
        subset = pd.pivot_table(
            subset, values=const.DEFAULT_NAME, index=["x", "y"], columns=["band"]
        ).reset_index()
        # Primary DGGSRS index
        cells = [
            self.dggrs.getZoneFromWGS84Centroid(resolution, dggal.GeoPoint(lon, lat))
            for lon, lat in zip(subset["y"], subset["x"])
        ]  # Vectorised
        dggrs_parent = [
            apply_n_reduce(self.dggrs.getZoneParents, resolution - parent_res, zone)
            for zone in cells
        ]
        subset = subset.drop(columns=["x", "y"])
        index_col = self.index_col(resolution)
        subset[index_col] = pd.Series(
            map(self.dggrs.getZoneTextID, cells), index=subset.index
        )
        partition_col = self.partition_col(parent_res)
        subset[partition_col] = pd.Series(
            map(self.dggrs.getZoneTextID, dggrs_parent), index=subset.index
        )
        # Rename bands
        bands = sdf["band"].unique()
        columns = dict(zip(bands, band_labels))
        subset = subset.rename(columns=columns)
        return pa.Table.from_pandas(subset)

    def cell_to_children_size(self, cell: str, desired_resolution: int) -> int:
        """
        Determine total number of children at some offset resolution

        Implementation of interface function.
        """
        current_resolution = self.dggrs.getZoneLevel(self.dggrs.getZoneFromTextID(cell))
        n = desired_resolution - current_resolution
        return self.refinementRatio**n

    @staticmethod
    def valid_set(cells: set) -> set[str]:
        """
        Implementation of interface function.
        """
        return set(filter(lambda c: (not pd.isna(c)), cells))

    def parent_cells(self, cells: set, resolution) -> map:
        """
        Implementation of interface function.
        """
        # TODO appropriately handle potential for multiple parentage in dggal (e.g. ISEAH3)
        child_resolution = self.dggrs.getZoneLevel(self.dggrs.getZoneFromTextID(next(iter(cells))))
        return map(
            lambda zone: self.dggrs.getZoneTextID(apply_n_reduce(
                self.dggrs.getZoneParents, child_resolution - resolution, self.dggrs.getZoneFromTextID(zone)
            )), cells
        )

    def expected_count(self, parent: str, resolution: int):
        """
        Implementation of interface function.
        """
        return self.cell_to_children_size(parent, resolution)

    def cell_to_point(self, cell: str) -> shapely.geometry.Point:
        geo_point : dggal.GeoPoint = self.dggrs.getZoneWGS84Centroid(self.dggrs.getZoneFromTextID(cell))
        return shapely.Point(
            geo_point.lon, geo_point.lat
        )

    def cell_to_polygon(self, cell: str) -> shapely.geometry.Polygon:
        geo_points : List[dggal.GeoPoint] = self.dggrs.getZoneWGS84Vertices(self.dggrs.getZoneFromTextID(cell))
        return shapely.Polygon(
            tuple([(p.lon, p.lat) for p in geo_points])
        )


# def ISEA7HRasterIndexer(DGGALRasterIndexer):
#     def __init__(self, dggs: str):
#         super().__init__(dggs)
#         self.dggrs = dggal.ISEA7H()

# NB  All zones of GNOSIS Global Grid and ISEA9R have single parents, whereas ISEA3H zones have one parent if they are a centroid child, and three parents otherwise if they are a vertex child.  See dggrs.getMaxParents()


class ISEA9RRasterIndexer(DGGALRasterIndexer):
    """
    A raster indexer for the ISEA9R DGGS, an axis-aligned and equal-area DGGH based on the Icosahedral Snyder Equal-Area (ISEA) planar projection using rhombuses with a refinement ratio of 9.
    """

    def __init__(self, dggs: str):
        super().__init__(dggs)
        dggal.pydggal_setup(dggal.Application(appGlobals=globals()))
        self._dggrs = dggal.ISEA9R()
        self._refinementRatio = self.dggrs.getRefinementRatio()

    @property
    def dggrs(self) -> dggal.DGGRS:
        return self._dggrs

    @property
    def refinementRatio(self) -> int:
        return self._refinementRatio

class ISEA7HRasterIndexer(DGGALRasterIndexer):
    """
    A raster indexer for the ISEA7H DGGS, an equal-area hexagonal grid with a refinement ratio of 7 defined in the ISEA projection.
    """

    def __init__(self, dggs: str):
        super().__init__(dggs)
        dggal.pydggal_setup(dggal.Application(appGlobals=globals()))
        self._dggrs = dggal.ISEA7H()
        self._refinementRatio = self.dggrs.getRefinementRatio()

    @property
    def dggrs(self) -> dggal.DGGRS:
        return self._dggrs

    @property
    def refinementRatio(self) -> int:
        return self._refinementRatio