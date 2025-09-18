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