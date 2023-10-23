from typing import Union

import pandas as pd
import geopandas as gpd

import raster2dggs.rhealpixdggs_py.rhealpixdggs as rhp_py

AnyDataFrame = Union[pd.DataFrame, gpd.GeoDataFrame]


@pd.api.extensions.register_dataframe_accessor("rHP")
class rHPAccessor:
    def __init__(self, df: pd.DataFrame) -> None:
        self._df = df

    def geo_to_hrp(
        self,
        resolution: int,
        lat_col: str = "lat",
        lng_col: str = "lng",
        set_index: bool = True,
    ) -> AnyDataFrame:
        """
        Adds rHEALPix index to (Geo)DataFrame

        pd.DataFrame: uses `lat_col` and `lng_col` (default `lat` and `lng`)
        gpd.GeoDataFrame: uses `geometry`

        resolution : int
            rHEALPix resolution
        lat_col : str
            Name of the latitude column (if used), default 'lat'
        lng_col : str
            Name of the longitude column (if used), default 'lng'
        set_index : bool
            If True, the columns with rHEALPix addresses is set as index, default 'True'

        Returns
        -------
        (Geo)DataFrame with rHEALPix addresses added
        """

        # DataFrame wrangling
        if isinstance(self._df, gpd.GeoDataFrame):
            lngs = self._df.geometry.x
            lats = self._df.geometry.y
        else:
            lngs = self._df[lng_col]
            lats = self._df[lat_col]

        # Index conversion
        rhpaddresses = [
            rhp_py.geo_to_rhp(lat, lng, resolution) for lat, lng in zip(lats, lngs)
        ]

        # Add results to DataFrame
        colname = f"rhp_{resolution}"
        assign_arg = {colname: rhpaddresses}
        df = self._df.assign(**assign_arg)
        if set_index:
            return df.set_index(colname)
        return df
