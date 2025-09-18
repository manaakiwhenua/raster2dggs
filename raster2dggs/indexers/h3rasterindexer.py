"""
@author: ndemaio
"""

from numbers import Number
from typing import Callable, Tuple, Union

import h3pandas  # Necessary import despite lack of explicit use

import h3 as h3py
import pandas as pd
import pyarrow as pa
import xarray as xr
import numpy as np

import raster2dggs.constants as const

from raster2dggs.interfaces import RasterIndexer


class H3RasterIndexer(RasterIndexer):
    '''
    Class description here
    '''
    
    def index_func(    
            self,
            sdf: xr.DataArray,
            resolution: int,
            parent_res: int,
            nodata: Number = np.nan,
            band_labels: Tuple[str] = None,
            ) -> pa.Table:
        """
        Index a raster window to H3.
        Subsequent steps are necessary to resolve issues at the boundaries of windows.
        If windows are very small, or in strips rather than blocks, processing may be slower
        than necessary and the recommendation is to write different windows in the source raster.
        
        Implementation of interface function.
        """
        sdf: pd.DataFrame = sdf.to_dataframe().drop(columns=["spatial_ref"]).reset_index()
        subset: pd.DataFrame = sdf.dropna()
        subset = subset[subset.value != nodata]
        subset = pd.pivot_table(
            subset, values=const.DEFAULT_NAME, index=["x", "y"], columns=["band"]
        ).reset_index()
        # Primary H3 index
        h3index = subset.h3.geo_to_h3(resolution, lat_col="y", lng_col="x").drop(
            columns=["x", "y"]
        )
        # Secondary (parent) H3 index, used later for partitioning
        h3index = h3index.h3.h3_to_parent(parent_res).reset_index()
        # Renaming columns to actual band labels
        bands = sdf["band"].unique()
        band_names = dict(zip(bands, map(lambda i: band_labels[i - 1], bands)))
        for k, v in band_names.items():
            if band_names[k] is None:
                band_names[k] = str(bands[k - 1])
            else:
                band_names = band_names
        h3index = h3index.rename(columns=band_names)
        return pa.Table.from_pandas(h3index)
        
    
    def parent_groupby(
            self,
            df,
            resolution: int,
            aggfunc: Union[str, Callable],
            decimals: int
            ) -> pd.DataFrame:
        """
        Function for aggregating the h3 resolution values per parent partition. Each partition will be run through with a
        pandas .groupby function. This step is to ensure there are no duplicate h3 values, which will happen when indexing a
        high resolution raster at a coarser h3 resolution.

        Implementation of interface function.
        """
        PAD_WIDTH = const.zero_padding("h3")
        
        if decimals > 0:
            return df.groupby(f"h3_{resolution:0{PAD_WIDTH}d}").agg(aggfunc).round(decimals)
        else:
            return (
                df.groupby(f"h3_{resolution:0{PAD_WIDTH}d}")
                .agg(aggfunc)
                .round(decimals)
                .astype("Int64")
            )
        
        
    def cell_to_children_size(
            self,
            cell,
            desired_resolution: int
            ) -> int:
        """
        Determine total number of children at some offset resolution

        Implementation of interface function.
        """
        current_resolution = h3py.get_resolution(cell)
        n = desired_resolution - current_resolution
        if h3py.is_pentagon(cell):
            return 1 + 5 * (7**n - 1) // 6
        else:
            return 7**n
        
        
    def compaction(
            self,
            df: pd.DataFrame,
            resolution: int,
            parent_res: int
            ) -> pd.DataFrame:
        """
        Returns a compacted version of the input dataframe.
        Compaction only occurs if all values (i.e. bands) of the input share common values across all sibling cells.
        Compaction will not be performed beyond parent_res or resolution.
        It assumes and requires that the input has unique DGGS cell values as the index.
        
        Implementation of interface function.
        """
        unprocessed_indices = set(
            filter(lambda c: (not pd.isna(c)) and h3py.is_valid_cell(c), set(df.index))
        )
        if not unprocessed_indices:
            return df
        compaction_map = {}
        for r in range(parent_res, resolution):
            parent_cells = map(lambda x: h3py.cell_to_parent(x, r), unprocessed_indices)
            parent_groups = df.loc[list(unprocessed_indices)].groupby(list(parent_cells))
            for parent, group in parent_groups:
                if parent in compaction_map:
                    continue
                expected_count = self.cell_to_children_size(parent, resolution)
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