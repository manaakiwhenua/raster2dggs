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
    Provides a base class and interface for all indexers integrating a specific
    DGGS. It should never be instantiated directly because all methods raise
    a NotImplementedError by design - the only thing that's not abstract is
    the DGGS name string as defined in the click command. The methods should
    be implemented by the child classes deriving from this interface instead.
    '''
    
    def __init__(self, dggs: str):
        '''
        Value used across all child classes
        '''
        self.dggs = dggs
    
    
    def index_func(
            self,
            sdf: xr.DataArray,
            resolution: int,
            parent_res: int,
            nodata: Number = np.nan,
            band_labels: Tuple[str] = None,
            ) -> pa.Table:
        '''
        Needs to be implemented by child class
        '''
        raise NotImplementedError()
        
        
    def parent_groupby(
            self,
            df,
            resolution: int,
            aggfunc: Union[str, Callable],
            decimals: int
            ) -> pd.DataFrame:
        '''
        Needs to be implemented by child class
        '''
        raise NotImplementedError()
        
        
    def cell_to_children_size(
            self,
            cell,
            desired_resolution: int
            ) -> int:
        '''
        Needs to be implemented by child class
        '''
        raise NotImplementedError()
        
        
    def compaction(
            self,
            df: pd.DataFrame,
            resolution: int,
            parent_res: int
            ) -> pd.DataFrame:
        '''
        Needs to be implemented by child class
        '''
        raise NotImplementedError()