"""
@author: ndemaio
"""

from numbers import Number
from typing import Callable, Tuple, Union

import pandas as pd
import pyarrow as pa
import xarray as xr
import numpy as np


class RasterIndexer:
    '''
    Interface description here
    '''
    
    def __init__(self, dggs: str):
        self.dggs = dggs
    
    
    def index_func(
            self,
            sdf: xr.DataArray,
            resolution: int,
            parent_res: int,
            nodata: Number = np.nan,
            band_labels: Tuple[str] = None,
            ) -> pa.Table:
        raise NotImplementedError()
        
        
    def parent_groupby(
            self,
            df,
            resolution: int,
            aggfunc: Union[str, Callable],
            decimals: int
            ) -> pd.DataFrame:
        raise NotImplementedError()
        
        
    def cell_to_children_size(
            self,
            cell,
            desired_resolution: int
            ) -> int:
        raise NotImplementedError()
        
        
    def compaction(
            self,
            df: pd.DataFrame,
            resolution: int,
            parent_res: int
            ) -> pd.DataFrame:
        raise NotImplementedError()