"""
@author: ndemaio
"""

from numbers import Number
from typing import Callable, Tuple, Union
from s2sphere import LatLng, CellId

import pandas as pd
import pyarrow as pa
import xarray as xr
import numpy as np

import raster2dggs.constants as const

from raster2dggs.interfaces import RasterIndexer


class S2RasterIndexer(RasterIndexer):
    """
    Provides integration for Google's S2 DGGS.
    """

    def index_func(
        self,
        sdf: xr.DataArray,
        resolution: int,
        parent_res: int,
        nodata: Number = np.nan,
        band_labels: Tuple[str] = None,
    ) -> pa.Table:
        """
        Index a raster window to S2.
        Subsequent steps are necessary to resolve issues at the boundaries of windows.
        If windows are very small, or in strips rather than blocks, processing may be slower
        than necessary and the recommendation is to write different windows in the source raster.

        Implementation of interface function.
        """
        PAD_WIDTH = const.zero_padding("s2")

        sdf: pd.DataFrame = (
            sdf.to_dataframe().drop(columns=["spatial_ref"]).reset_index()
        )
        subset: pd.DataFrame = sdf.dropna()
        subset = subset[subset.value != nodata]
        subset = pd.pivot_table(
            subset, values=const.DEFAULT_NAME, index=["x", "y"], columns=["band"]
        ).reset_index()
        # S2 index
        cells = [
            CellId.from_lat_lng(LatLng.from_degrees(lat, lon))
            for lat, lon in zip(subset["y"], subset["x"])
        ]
        s2 = [cell.parent(resolution).to_token() for cell in cells]
        s2_parent = [cell.parent(parent_res).to_token() for cell in cells]
        subset = subset.drop(columns=["x", "y"])
        subset[f"s2_{resolution:0{PAD_WIDTH}d}"] = pd.Series(s2, index=subset.index)
        subset[f"s2_{parent_res:0{PAD_WIDTH}d}"] = pd.Series(
            s2_parent, index=subset.index
        )
        # Renaming columns to actual band labels
        bands = sdf["band"].unique()
        columns = dict(zip(bands, band_labels))
        subset = subset.rename(columns=columns)
        return pa.Table.from_pandas(subset)

    def parent_groupby(
        self,
        df: pd.DataFrame,
        resolution: int,
        parent_res: int,
        aggfunc: Union[str, Callable],
        decimals: int,
    ) -> pd.DataFrame:
        """
        Function for aggregating the S2 resolution values per parent partition. Each partition will be run through with a
        pandas .groupby function. This step is to ensure there are no duplicate S2 values, which will happen when indexing a
        high resolution raster at a coarser S2 resolution.

        Implementation of interface function.
        """
        PAD_WIDTH = const.zero_padding("s2")
        index_col = f"s2_{resolution:0{PAD_WIDTH}d}"
        partition_col = f"s2_{parent_res:0{PAD_WIDTH}d}"
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

    def cell_to_children_size(self, cell, desired_resolution: int) -> int:
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

    def compaction(
        self, df: pd.DataFrame, resolution: int, parent_res: int
    ) -> pd.DataFrame:
        """
        Returns a compacted version of the input dataframe.
        Compaction only occurs if all values (i.e. bands) of the input share common values across all sibling cells.
        Compaction will not be performed beyond parent_res or resolution.
        It assumes and requires that the input has unique DGGS cell values as the index.

        Implementation of interface function.
        """
        unprocessed_indices = set(
            map(
                lambda c: c.to_token(),
                filter(
                    lambda c: c.is_valid(),
                    map(
                        lambda c: CellId.from_token(c),
                        filter(lambda c: not pd.isna(c), set(df.index)),
                    ),
                ),
            )
        )
        if not unprocessed_indices:
            return df
        compaction_map = {}
        for r in range(parent_res, resolution):
            parent_cells = map(
                lambda token: CellId.from_token(token).parent(r).to_token(),
                unprocessed_indices,
            )
            parent_groups = df.loc[list(unprocessed_indices)].groupby(
                list(parent_cells)
            )
            for parent, group in parent_groups:
                if isinstance(parent, tuple) and len(parent) == 1:
                    parent = parent[0]
                if parent in compaction_map:
                    continue
                expected_count = self.cell_to_children_size(
                    CellId.from_token(parent), resolution
                )
                if len(group) == expected_count and all(group.nunique() == 1):
                    compact_row = group.iloc[0]
                    compact_row.name = parent  # Rename the index to the parent cell
                    compaction_map[parent] = compact_row
                    unprocessed_indices -= set(group.index)
        compacted_df = pd.DataFrame(list(compaction_map.values()))
        remaining_df = df.loc[list(unprocessed_indices)]
        result_df = pd.concat([compacted_df, remaining_df])
        result_df = result_df.rename_axis(df.index.name)
        return result_df
