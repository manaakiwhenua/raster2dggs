from typing import Union, Callable, Any
from functools import partial, update_wrapper

import shapely
import pandas as pd
import geopandas as gpd

import rhealpixdggs.rhp_wrappers as rhp_py

AnyDataFrame = Union[pd.DataFrame, gpd.GeoDataFrame]


def wrapped_partial(func, *args, **kwargs):
    """
    Properly wrapped partial function

    Appropriated from h3pandas.util package
    """
    partial_func = partial(func, *args, **kwargs)
    update_wrapper(partial_func, func)
    return partial_func


@pd.api.extensions.register_dataframe_accessor("rHP")
class rHPAccessor:
    """
    Shamelessly appropriated from equivalent class in h3pandas package

    WIP: adapt more functions as needed
    """

    def __init__(self, df: pd.DataFrame) -> None:
        self._df = df

    def geo_to_rhp(
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
            rhp_py.geo_to_rhp(lat, lng, resolution, False)
            for lat, lng in zip(lats, lngs)
        ]

        # Add results to DataFrame
        colname = f"rhp_{resolution:02}"
        assign_arg = {colname: rhpaddresses}
        df = self._df.assign(**assign_arg)
        if set_index:
            return df.set_index(colname)
        return df

    def rhp_to_parent(self, resolution: int = None) -> AnyDataFrame:
        """
        Parameters
        ----------
        resolution : int or None
            rHEALPix resolution. If None, then returns the direct parent of each rHEALPix cell.
        """
        column = f"rhp_{resolution:02}" if resolution is not None else "rhp_parent"

        return self._apply_index_assign(
            wrapped_partial(rhp_py.rhp_to_parent, res=resolution), column
        )

    def rhp_to_geo_boundary(self) -> AnyDataFrame:
        """Add `geometry` with rHEALPix squares to the DataFrame. Assumes rHEALPix index.

        Returns
        -------
        GeoDataFrame with rHEALPix geometry
        """
        return self._apply_index_assign(
            wrapped_partial(rhp_py.rhp_to_geo_boundary, geo_json=True, plane=False),
            "geometry",
            lambda x: shapely.geometry.Polygon(x),
            lambda x: gpd.GeoDataFrame(
                x, crs="epsg:4326"
            ),  # TODO: add correct coordinate system?
        )

    def _apply_index_assign(
        self,
        func: Callable,
        column_name: str,
        processor: Callable = lambda x: x,
        finalizer: Callable = lambda x: x,
    ) -> Any:
        """
        Helper method. Applies `func` to index and assigns the result to `column`.

        Parameters
        ----------
        func : Callable
            single-argument function to be applied to each rHEALPix address
        column_name : str
            name of the resulting column
        processor : Callable
            (Optional) further processes the result of func. Default: identity
        finalizer : Callable
            (Optional) further processes the resulting dataframe. Default: identity

        Returns
        -------
        Dataframe with column `column` containing the result of `func`.
        If using `finalizer`, can return anything the `finalizer` returns.
        """
        result = [processor(func(rhpaddress)) for rhpaddress in self._df.index]
        assign_args = {column_name: result}

        return finalizer(self._df.assign(**assign_args))
