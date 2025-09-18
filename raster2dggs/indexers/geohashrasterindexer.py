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
            resolution: int,
            parent_res: int,
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
            gh.encode(lat, lon, precision=resolution)
            for lat, lon in zip(subset["y"], subset["x"])
        ]  # Vectorised
        # Secondary (parent) Geohash index, used later for partitioning
        geohash_parent = [gh[:parent_res] for gh in geohash]
        subset = subset.drop(columns=["x", "y"])
        subset[f"geohash_{resolution:0{PAD_WIDTH}d}"] = pd.Series(
            geohash, index=subset.index
        )
        subset[f"geohash_{parent_res:0{PAD_WIDTH}d}"] = pd.Series(
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
            resolution: int,
            aggfunc: Union[str, Callable],
            decimals: int
            ) -> pd.DataFrame:
        # TODO
        pass
        
    def cell_to_children_size(
            self,
            cell,
            desired_resolution: int
            ) -> int:
        # TODO
        pass
        
    def compaction(
            self,
            df: pd.DataFrame,
            resolution: int,
            parent_res: int
            ) -> pd.DataFrame:
        # TODO
        pass