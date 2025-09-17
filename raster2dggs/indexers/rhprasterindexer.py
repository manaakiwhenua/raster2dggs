from interfaces import RasterIndexer

class RHPRasterIndexer(RasterIndexer):
    '''
    Class description here
    '''
    def index_func(    
            sdf: xr.DataArray,
            resolution: int,
            parent_res: int,
            nodata: Number = np.nan,
            band_labels: Tuple[str] = None,
            ) -> pa.Table:
        # TODO
        pass
        
    def parent_groupby(
            df,
            resolution: int,
            aggfunc: Union[str, Callable],
            decimals: int
            ) -> pd.DataFrame:
        # TODO
        pass
        
    def compaction(
            df: pd.DataFrame,
            resolution: int,
            parent_res: int
            ) -> pd.DataFrame:
        # TODO
        pass