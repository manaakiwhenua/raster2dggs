"""
@author: ndemaio
"""

from numbers import Number
from typing import Callable, Tuple, Union

import a5 as a5py
import pandas as pd
import pyarrow as pa
import xarray as xr
import numpy as np

import raster2dggs.constants as const

from raster2dggs.interfaces import RasterIndexer


def is_valid_a5_cell(cell: str, min_resolution: int, max_resolution: int):
    cell = a5py.hex_to_u64(cell)
    try:
        c: a5py.A5Cell = a5py.core.serialization.deserialize(cell)
    except TypeError:
        return False
    if not (min_resolution <= c["resolution"] <= max_resolution):
        return False
    return True


class A5RasterIndexer(RasterIndexer):
    """
    Provides integration for the A5 DGGS.
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
        Index a raster window to A5.
        Subsequent steps are necessary to resolve issues at the boundaries of windows.
        If windows are very small, or in strips rather than blocks, processing may be slower
        than necessary and the recommendation is to write different windows in the source raster.

        Implementation of interface function.
        """
        PAD_WIDTH = const.zero_padding("a5")

        sdf: pd.DataFrame = (
            sdf.to_dataframe().drop(columns=["spatial_ref"]).reset_index()
        )
        subset: pd.DataFrame = sdf.dropna()
        subset = subset[subset.value != nodata]
        subset = pd.pivot_table(
            subset, values=const.DEFAULT_NAME, index=["x", "y"], columns=["band"]
        ).reset_index()
        cells = [
            a5py.lonlat_to_cell((lon, lat), resolution)
            for lon, lat in zip(subset["x"], subset["y"])
        ]  # NB a5py.lonlat_to_cell is quite slow
        # Secondary (parent) A5 index, used later for partitioning
        a5_parent = [a5py.cell_to_parent(cell, parent_res) for cell in cells]
        subset = subset.drop(columns=["x", "y"])
        subset[f"a5_{resolution:0{PAD_WIDTH}d}"] = pd.Series(
            map(a5py.u64_to_hex, cells), index=subset.index
        )
        subset[f"a5_{parent_res:0{PAD_WIDTH}d}"] = pd.Series(
            map(a5py.u64_to_hex, a5_parent), index=subset.index
        )
        # Renaming columns to actual band labels
        bands = sdf["band"].unique()
        band_names = dict(zip(bands, map(lambda i: band_labels[i - 1], bands)))
        for k, v in band_names.items():
            if band_names[k] is None:
                band_names[k] = str(bands[k - 1])
            else:
                band_names = band_names
        subset = subset.rename(columns=band_names)
        return pa.Table.from_pandas(subset)

    def parent_groupby(
        self, df, resolution: int, aggfunc: Union[str, Callable], decimals: int
    ) -> pd.DataFrame:
        """
        Function for aggregating the A5 resolution values per parent partition. Each partition will be run through with a
        pandas .groupby function. This step is to ensure there are no duplicate A5 values, which will happen when indexing a
        high resolution raster at a coarser A5 resolution.

        Implementation of interface function.
        """
        PAD_WIDTH = const.zero_padding("a5")

        if decimals > 0:
            return (
                df.groupby(f"a5_{resolution:0{PAD_WIDTH}d}")
                .agg(aggfunc)
                .round(decimals)
            )
        else:
            return (
                df.groupby(f"a5_{resolution:0{PAD_WIDTH}d}")
                .agg(aggfunc)
                .round(decimals)
                .astype("Int64")
            )

    def cell_to_children_size(self, cell: int, desired_resolution: int) -> int:
        """
        Determine total number of children at some offset resolution

        Implementation of interface function.
        """
        cell_level = a5py.get_resolution(cell)
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
            filter(
                lambda c: (not pd.isna(c))
                and is_valid_a5_cell(c, parent_res, resolution),
                set(df.index),
            ),
        )
        if not unprocessed_indices:
            return df
        compaction_map = {}
        for r in range(parent_res, resolution):
            parent_cells = map(
                a5py.u64_to_hex,
                map(
                    lambda x: a5py.cell_to_parent(x, r),
                    map(a5py.hex_to_u64, unprocessed_indices),
                ),
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
                    a5py.hex_to_u64(parent), resolution
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
