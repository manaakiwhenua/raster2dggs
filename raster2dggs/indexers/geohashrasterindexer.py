"""
@author: ndemaio
"""

from numbers import Number
from typing import Callable, Tuple, Union

import geohash as gh

import pandas as pd
import pyarrow as pa
import xarray as xr
import numpy as np

import raster2dggs.constants as const

from raster2dggs.interfaces import RasterIndexer

class GeohashRasterIndexer(RasterIndexer):
    '''
    Class description here
    '''
    def index_func(    
            self,
            sdf: xr.DataArray,
            precision: int,
            parent_precision: int,
            nodata: Number = np.nan,
            band_labels: Tuple[str] = None,
            ) -> pa.Table:
        """
        Index a raster window to Geohash.
        Subsequent steps are necessary to resolve issues at the boundaries of windows.
        If windows are very small, or in strips rather than blocks, processing may be slower
        than necessary and the recommendation is to write different windows in the source raster.
        """
        PAD_WIDTH = const.zero_padding("geohash")
        
        sdf: pd.DataFrame = sdf.to_dataframe().drop(columns=["spatial_ref"]).reset_index()
        subset: pd.DataFrame = sdf.dropna()
        subset = subset[subset.value != nodata]
        subset = pd.pivot_table(
            subset, values=const.DEFAULT_NAME, index=["x", "y"], columns=["band"]
        ).reset_index()
        # Primary Geohash index
        geohash = [
            gh.encode(lat, lon, precision=precision)
            for lat, lon in zip(subset["y"], subset["x"])
        ]  # Vectorised
        # Secondary (parent) Geohash index, used later for partitioning
        geohash_parent = [gh[:parent_precision] for gh in geohash]
        subset = subset.drop(columns=["x", "y"])
        subset[f"geohash_{precision:0{PAD_WIDTH}d}"] = pd.Series(
            geohash, index=subset.index
        )
        subset[f"geohash_{parent_precision:0{PAD_WIDTH}d}"] = pd.Series(
            geohash_parent, index=subset.index
        )
        # Rename bands
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
            self,
            df,
            precision: int,
            aggfunc: Union[str, Callable],
            decimals: int
            ) -> pd.DataFrame:
        """
        Function for aggregating the Geohash values per parent partition. Each partition will be run through with a
        pandas .groupby function. This step is to ensure there are no duplicate Geohashes, which will happen when indexing a
        high resolution raster at a coarse Geohash precision.
        """
        PAD_WIDTH = const.zero_padding("geohash")

        if decimals > 0:
            return (
                df.groupby(f"geohash_{precision:0{PAD_WIDTH}d}")
                .agg(aggfunc)
                .round(decimals)
            )
        else:
            return (
                df.groupby(f"geohash_{precision:0{PAD_WIDTH}d}")
                .agg(aggfunc)
                .round(decimals)
                .astype("Int64")
            )
        
    def cell_to_children_size(
            self,
            cell,
            desired_level: int
            ) -> int:
        """
        Determine total number of children at some offset resolution
        """
        level = len(cell)
        if desired_level < level:
            return 0
        return 32 ** (desired_level - level)
        
    def compaction(
            self,
            df: pd.DataFrame,
            precision: int,
            parent_precision: int
            ) -> pd.DataFrame:
        """
        Returns a compacted version of the input dataframe.
        Compaction only occurs if all values (i.e. bands) of the input share common values across all sibling cells.
        Compaction will not be performed beyond parent_level or level.
        It assumes and requires that the input has unique DGGS cell values as the index.
        """
        unprocessed_indices = set(filter(lambda c: not pd.isna(c), set(df.index)))
        if not unprocessed_indices:
            return df
        compaction_map = {}
        for p in range(parent_precision, precision):
            parent_cells = list(
                map(lambda gh: self.to_parent(gh, p), unprocessed_indices)
            )
            parent_groups = df.loc[list(unprocessed_indices)].groupby(list(parent_cells))
            for parent, group in parent_groups:
                if parent in compaction_map:
                    continue
                expected_count = self.cell_to_children_size(parent, precision)
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
    
    def to_parent(self, cell: str, desired_precision: int) -> str:
        """
        Returns cell parent at some offset level.
        """
        return cell[:desired_precision]