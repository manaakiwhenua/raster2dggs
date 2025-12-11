"""
@author: ndemaio
"""

from numbers import Number
from typing import Callable, Tuple, Union

import pandas as pd
import pyarrow as pa
import xarray as xr
import numpy as np
import shapely


class IRasterIndexer:
    """
    Provides a base class and interface for all indexers integrating a
        specific DGGS. It should never be instantiated directly because
        all methods raise a NotImplementedError by design. The methods
        should be implemented by the child classes deriving from this
        interface instead.
    """

    def __init__(self, dggs: str):
        """
        Value used across all child classes
        """
        self.dggs = dggs

    def index_col(self, resolution: int) -> str:
        """
        Returns the primary DGGS index column name, with zero padding so that column
        names across a DGGS' full resolution space have the same length.
        """
        raise NotImplementedError()

    def partition_col(self, parent_resolution: int) -> str:
        """
        Returns the partition DGGS index column name, with zero padding so that column
        names across a DGGS' full resolution space have the same length.
        """
        raise NotImplementedError()

    def band_cols(self, df: pd.DataFrame) -> list[str]:
        """
        Returns the column names representing raster bands from an input image.
        """
        raise NotImplementedError()

    @staticmethod
    def valid_set(cells: set) -> set:
        """
        Given a set of DGGS cells of the same DGGS return the subset that are valid cell addresses.
        """
        raise NotImplementedError()

    @staticmethod
    def parent_cells(cells: set, resolution: int) -> map:
        """
        Given a set of DGGS cells, return an iterable of parent cells at given resolution
        """
        raise NotImplementedError

    def expected_count(self, parent: str, resolution: int) -> int:
        """
        Given a DGGS (parent) cell ID, and a target (child) resolution,
        return the expected number of child cells that completel represent this
        parent cell at the target resolution.
        """
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
        Function for primary indexation.
        """
        raise NotImplementedError()

    def parent_groupby(
        self,
        df,
        resolution: int,
        parent_res: int,
        aggfunc: Union[str, Callable],
        decimals: int,
    ) -> pd.DataFrame:
        """
        Function for aggregating the DGGS resolution values per parent
            partition. Each partition will be run through with a pandas
            groupby function. This step is to ensure there are no duplicate
            cell values, which will happen when indexing a high resolution
            raster at a coarser DGGS resolution.
        """
        raise NotImplementedError

    @staticmethod
    def cell_to_children_size(cell, desired_resolution: int) -> int:
        """
        Needs to be implemented by child class
        """
        raise NotImplementedError()

    @staticmethod
    def cell_to_point(cell: str) -> shapely.geometry.Point:
        """
        Needs to be implemented by child class
        """
        raise NotImplementedError()

    @staticmethod
    def cell_to_polygon(cell: str) -> shapely.geometry.Polygon:
        """
        Needs to be implemented by child class
        """
        raise NotImplementedError()

    def compaction(
        self, df: pd.DataFrame, resolution: int, parent_res: int
    ) -> pd.DataFrame:
        """
        Returns a compacted version of the input dataframe.
        Compaction only occurs if all values (i.e. bands) of the input
            share common values across all sibling cells.
        Compaction will not be performed beyond parent_res.
        It assumes that the input has unique DGGS cell values
            as the index.
        """
        raise NotImplementedError()
