"""
@author: ndemaio
"""

from numbers import Number
from typing import Callable, Tuple, Union

import maidenhead as mh
import pandas as pd
import pyarrow as pa
import xarray as xr
import numpy as np

import raster2dggs.constants as const

from raster2dggs.interfaces import RasterIndexer


class MaidenheadRasterIndexer(RasterIndexer):
    """
    Provides integration for Maidenhead Locator System geocodes.
    """

    def index_func(
        self,
        sdf: xr.DataArray,
        precision: int,
        parent_precision: int,
        nodata: Number = np.nan,
        band_labels: Tuple[str] = None,
    ) -> pa.Table:
        """
        Index a raster window to Maidenhead.
        Subsequent steps are necessary to resolve issues at the boundaries of windows.
        If windows are very small, or in strips rather than blocks, processing may be slower
        than necessary and the recommendation is to write different windows in the source raster.

        Implementation of interface function.
        """
        PAD_WIDTH = const.zero_padding("maidenhead")

        sdf: pd.DataFrame = (
            sdf.to_dataframe().drop(columns=["spatial_ref"]).reset_index()
        )
        subset: pd.DataFrame = sdf.dropna()
        subset = subset[subset.value != nodata]
        subset = pd.pivot_table(
            subset, values=const.DEFAULT_NAME, index=["x", "y"], columns=["band"]
        ).reset_index()
        # Primary Maidenhead index
        maidenhead = [
            mh.to_maiden(lat, lon, precision)
            for lat, lon in zip(subset["y"], subset["x"])
        ]  # Vectorised
        # Secondary (parent) Maidenhead index, used later for partitioning
        maidenhead_parent = [
            self.cell_to_parent(mh, parent_precision) for mh in maidenhead
        ]
        subset = subset.drop(columns=["x", "y"])
        subset[f"maidenhead_{precision:0{PAD_WIDTH}d}"] = pd.Series(
            maidenhead, index=subset.index
        )
        subset[f"maidenhead_{parent_precision:0{PAD_WIDTH}d}"] = pd.Series(
            maidenhead_parent, index=subset.index
        )
        # Rename bands
        bands = sdf["band"].unique()
        columns = dict(zip(bands, band_labels))
        subset = subset.rename(columns=columns)
        return pa.Table.from_pandas(subset)

    def parent_groupby(
        self, df, precision: int, aggfunc: Union[str, Callable], decimals: int
    ) -> pd.DataFrame:
        """
        Function for aggregating the Maidenhead values per parent partition. Each partition will be run through with a
        pandas .groupby function. This step is to ensure there are no duplicate Maidenhead indices, which will certainly happen when indexing most raster datasets as Maidenhead has low precision.

        Implementation of interface function.
        """
        PAD_WIDTH = const.zero_padding("maidenhead")

        if decimals > 0:
            return (
                df.groupby(f"maidenhead_{precision:0{PAD_WIDTH}d}")
                .agg(aggfunc)
                .round(decimals)
            )
        else:
            return (
                df.groupby(f"maidenhead_{precision:0{PAD_WIDTH}d}")
                .agg(aggfunc)
                .round(decimals)
                .astype("Int64")
            )

    def cell_to_children_size(self, cell, desired_level: int) -> int:
        """
        Determine total number of children at some offset level.

        Implementation of interface function.
        """
        level = len(cell) // 2
        if desired_level < level:
            return 0
        return 100 ** (desired_level - level)

    def compaction(
        self, df: pd.DataFrame, level: int, parent_level: int
    ) -> pd.DataFrame:
        """
        Returns a compacted version of the input dataframe.
        Compaction only occurs if all values (i.e. bands) of the input share common values across all sibling cells.
        Compaction will not be performed beyond parent_level or level.
        It assumes and requires that the input has unique DGGS cell values as the index.

        Implementation of interface function.
        """
        unprocessed_indices = set(
            filter(lambda c: not pd.isna(c) and len(c) >= 2, set(df.index))
        )
        if not unprocessed_indices:
            return df
        compaction_map = {}
        for l in range(parent_level, level):
            parent_cells = list(
                map(lambda x: self.cell_to_parent(x, l), unprocessed_indices)
            )
            parent_groups = df.loc[list(unprocessed_indices)].groupby(
                list(parent_cells)
            )
            for parent, group in parent_groups:
                if isinstance(parent, tuple) and len(parent) == 1:
                    parent = parent[0]
                if parent in compaction_map:
                    continue
                expected_count = self.cell_to_children_size(parent, level)
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

    def cell_to_parent(self, cell: str, parent_level: int) -> str:
        """
        Returns cell parent at some offset level.

        Not a part of the RasterIndexer interface.
        """
        return cell[: parent_level * 2]
