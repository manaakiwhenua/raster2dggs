import logging
import numpy as np
from pathlib import Path
from typing import Union

import click
import click_log
import dask.dataframe as dd
import dask_geopandas as dgpd
import geopandas as gpd
import pandas as pd
import rasterio as rio
from rasterio.vrt import WarpedVRT

from raster2dggs import __version__

LOGGER = logging.getLogger(__name__)
click_log.basic_config(LOGGER)

@click.command(context_settings={"show_default": True})
@click_log.simple_verbosity_option(LOGGER)
@click.argument("raster_input", type=click.Path(), nargs=1)
@click.argument("output_directory", type=click.Path(), nargs=1)
@click.option(
    "-p",
    "--precision",
    required=True,
    type=click.IntRange(1,12),
    default=5,
    help="Geohash precision (character count)"
)
@click.option(
    "-s",
    "--string",
    required=True,
    type=bool,
    default=True,
    help="To use string or integer Geohash"
)
@click.version_option(version=__version__)
def geohash(
    raster_input: Union[str, Path],
    output_directory: Union[str, Path],
    precision: int,
    string: bool,
):
    """
    Ingest a raster image and index it using Geohash.

    RASTER_INPUT is the path to input raster data; prepend with protocol like s3:// or hdfs:// for remote data.
    OUTPUT_DIRECTORY should be a directory, not a file, as it will be the write location for an Apache Parquet data store, with partitions equivalent to parent cells of target cells at a fixed offset. However, this can also be remote (use the appropriate prefix, e.g. s3://).
    """
    with rio.Env(CHECK_WITH_INVERT_PROJ=True):
        with rio.open(raster_input) as src:
            with WarpedVRT(
                src, src_crs=src.crs, crs=rio.crs.CRS.from_epsg(4326)
            ) as vrt:
                transform = vrt.transform
                data = vrt.read(1) # TODO more bands
                rows, cols = np.indices(data.shape)
                xs, ys = rio.transform.xy(transform, rows, cols)
                xs = np.array(xs).flatten()
                ys = np.array(ys).flatten()
                data = data.flatten()
                gdf = gpd.GeoDataFrame({
                    "geometry": gpd.points_from_xy(xs, ys),
                    "B01": data
                }, crs=vrt.crs)

    ddf = dgpd.from_geopandas(gdf, npartitions=4)
    LOGGER.debug(ddf)

    ddf['geohash'] = ddf.geohash(as_string=string, precision=precision)
    
    aggregated_ddf = ddf.groupby('geohash').B01.agg(['mean', 'sum', 'count']).reset_index()

    aggregated_ddf.columns = ['geohash', 'mean', 'sum', 'count']

    def enforce_schema(df):
        df['mean'] = df['mean'].round(0).astype('int16')
        # df['sum'] = df['sum'].astype('int16')
        df['count'] = df['count'].astype('int16')
        return df

    aggregated_ddf = aggregated_ddf.map_partitions(enforce_schema, meta={
        'geohash': 'str',
        'mean': 'int16',
        'sum': 'int16',
        'count': 'int16'
    })

    aggregated_ddf.to_parquet(output_directory, engine="pyarrow")