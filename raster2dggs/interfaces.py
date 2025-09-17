# -*- coding: utf-8 -*-
import pandas as pd
import pyarrow as pa

class RasterIndexer:
    '''
    Interface description here
    '''
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
        
    def compaction(
            self,
            df: pd.DataFrame,
            resolution: int,
            parent_res: int
            ) -> pd.DataFrame:
        raise NotImplementedError()