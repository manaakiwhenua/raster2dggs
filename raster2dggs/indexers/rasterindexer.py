"""
@author: ndemaio
"""

from numbers import Number
from typing import Callable, Tuple, Union

import pandas as pd
import pyarrow as pa
import xarray as xr
import numpy as np

from .. import constants as const
from ..interfaces import IRasterIndexer


class RasterIndexer(IRasterIndexer):
    """
    Provides a partial implementation for raster indexers integrating a
        specific DGGS. It should never be instantiated directly because
        many methods raise a NotImplementedError by design. The methods
        should be implemented by the child classes deriving from this
        interface instead.
        If specialised behaviour is required, methods may be
        re-implemented by derived classes.
    """

    def __init__(self, dggs: str):
        """
        Value used across all child classes
        """
        self.dggs = dggs

    def __dask_tokenize__(self):
        """
        Only include stable, immutable fields that define behaviour
        """
        return (type(self).__name__, self.dggs)

    def index_col(self, resolution):
        pad_width = const.zero_padding(self.dggs)
        return f"{self.dggs}_{resolution:0{pad_width}d}"

    def partition_col(self, parent_resolution):
        pad_width = const.zero_padding(self.dggs)
        return f"{self.dggs}_{parent_resolution:0{pad_width}d}"

    def band_cols(self, df: pd.DataFrame):
        return [c for c in df.columns if not c.startswith(f"{self.dggs}_")]

    @staticmethod
    def valid_set(cells: set) -> set:
        """
        Needs to be implemented by child class
        """
        raise NotImplementedError()

    @staticmethod
    def parent_cells(cells: set, resolution) -> map:
        """
        Needs to be implemented by child class
        """
        raise NotImplementedError

    def expected_count(self, parent: str, resolution: int):
        """
        Needs to be implemented by child class
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
        Needs to be implemented by child class
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
        index_col = self.index_col(resolution)
        partition_col = self.partition_col(parent_res)
        df = df.set_index(index_col)
        if decimals > 0:
            gb = (
                df.groupby([partition_col, index_col], sort=False, observed=True)
                .agg(aggfunc)
                .round(decimals)
            )
        else:
            gb = (
                df.groupby([partition_col, index_col], sort=False, observed=True)
                .agg(aggfunc)
                .round(decimals)
                .astype("Int64")
            )
        # Move parent out to a column; keep child as the index
        # MultiIndex levels are [partition_col, index_col] in that order
        gb = gb.reset_index(level=0)  # parent -> column
        gb.index.name = index_col  # child remains index
        return gb

    @staticmethod
    def cell_to_children_size(cell, desired_resolution: int) -> int:
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
        unprocessed_indices = self.valid_set(set(df.index))
        if not unprocessed_indices:
            return df
        band_cols = self.band_cols(df)
        compaction_map = {}

        for r in range(parent_res, resolution):
            parent_cells = self.parent_cells(unprocessed_indices, r)
            parent_groups = df.loc[list(unprocessed_indices)].groupby(
                list(parent_cells)
            )
            for parent, group in parent_groups:
                if isinstance(parent, tuple) and len(parent) == 1:
                    parent = parent[0]
                if parent in compaction_map:
                    continue
                expected_count = self.expected_count(parent, resolution)
                if len(group) == expected_count and all(
                    group[band_cols].nunique() == 1
                ):
                    compact_row = group.iloc[0]
                    compact_row.name = parent  # Rename the index to the parent cell
                    compaction_map[parent] = compact_row
                    unprocessed_indices -= set(group.index)
        compacted_df = pd.DataFrame(list(compaction_map.values()))
        remaining_df = df.loc[list(unprocessed_indices)]
        result_df = pd.concat([compacted_df, remaining_df])
        result_df = result_df.rename_axis(df.index.name)
        return result_df
