"""
Ingest raster images and index it to the H3 DGGS.
Output is an Apache Parquet data store, with partitions equivalent to windows of the input raster.
"""
import concurrent.futures
import gc
import logging
import multiprocessing as mp
from numbers import Number
import numpy as np
from pathlib import Path
import shutil
import threading
from typing import Tuple

import click
import dask
import dask.dataframe as dd
import h3
from h3 import h3
import h3pandas # Necessary import despite lack of explicit use
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import rasterio as rio
from rasterio import crs
from rasterio.enums import Resampling
from rasterio.vrt import WarpedVRT
import rioxarray
import xarray as xr

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
LOGGER = logging.getLogger(__name__)

MIN_H3, MAX_H3 = 0, 15

VALUE : str = 'value'

ROUND_DECIMALS : int = 1

# AGG_FUNC = ['mean'] # TODO allow multiple e.g. ['mean', 'min', 'max', 'count']

BAND = 1 # TODO allow multiple bands, ideally obtaining statistics in one pass

def _get_parent_res(resolution : int) -> int:
    return max(MIN_H3, resolution - 8)

def _h3func(sdf : xr.DataArray, resolution : int, parent_resolution : int, nodata : Number = np.nan, band_labels : Tuple[str] = None) -> pa.Table:
    '''
    Index a raster window to H3.
    Subsequent steps are necessary to resolve issues at the boundaries of windows.
    If windows are very small, or in strips rather than blocks, processing may be slower
    than necessary and the recommendation is to write different windows in the source raster. 
    '''
    sdf : pd.DataFrame = sdf.to_dataframe().drop(columns=['spatial_ref']).reset_index()
    subset : pd.DataFrame = sdf.dropna()
    subset = pd.pivot_table(subset, values='value', index=['x','y'], columns=['band']).reset_index()
    # Primary H3 index
    h3index = subset.h3.geo_to_h3(resolution, lat_col='y', lng_col='x').drop(columns= ['x','y'])
    # Secondary (parent) H3 index, used later for partitioning
    h3index = h3index.h3.h3_to_parent(parent_resolution).reset_index()
    # Renaming columns to actual band labels
    bands = sdf['band'].unique()
    band_names = dict(zip(
        bands,
        map(lambda i: band_labels[i-1], bands)
    ))
    h3index = h3index.rename(columns=band_names)
    return pa.Table.from_pandas(h3index)

def _initial_index(raster_input: Path, resolution: int, compression: str, max_workers: int, warp_args: dict)  -> Path:

    parent_resolution = _get_parent_res(resolution)
    LOGGER.info("Indexing at H3 resolution %d, parent resolution %d", resolution, parent_resolution)

    output_path_indexed = Path(f'./tests/data/output/{resolution}/step-1') # TODO make tmp output dir an option, and use tmp dir by default
    output_path_indexed.mkdir(parents=True, exist_ok=False)

    with rio.open(raster_input) as src:
        LOGGER.debug('Source CRS: %s', src.crs)
        # VRT used to avoid additional disk use given the potential for reprojection to 4326 prior to H# indexing
        band_names = src.descriptions
        with WarpedVRT(src, src_crs=src.crs, **warp_args) as vrt:
            LOGGER.info('VRT CRS: %s', vrt.crs)
            da : xr.Dataset = rioxarray.open_rasterio(
                vrt,
                lock=dask.utils.SerializableLock(),
                masked=True,
                default_name='value',
                band_as_variable=False # True borks VRT warping https://github.com/corteva/rioxarray/issues/644
            ).chunk(
                **{'y':'auto', 'x':'auto'}
            )#.drop_vars(['spatial_ref'])

            windows = [window for _, window in vrt.block_windows()]
            LOGGER.debug('%d windows (the same number of partitions will be created)', len(windows))
           
            write_lock = threading.Lock()

            def process(window):
                sdf = da.rio.isel_window(window)
                    
                result = _h3func(sdf, resolution, parent_resolution, vrt.nodata, band_labels=band_names)
                
                with write_lock:
                    pq.write_to_dataset(result, root_path=output_path_indexed, compression=compression)

            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                executor.map(process, windows)

    LOGGER.info('Stage 1 (primary indexing) complete')
    return output_path_indexed

def _address_boundary_issues(pq_input: Path, resolution: int, compression: str) -> Path:

    parent_resolution = _get_parent_res(resolution)

    output_path_aggregated = Path(f'./tests/data/output/{resolution}/step-3')
    output_path_aggregated.mkdir(parents=True, exist_ok=True)

    ddf = dd.read_parquet(
        pq_input
    ).set_index( # Set index as parent cell
        f'h3_{parent_resolution:02}' if parent_resolution > 0 else 'h3_parent' # https://github.com/DahnJ/H3-Pandas/issues/15
    )

    # Count parents, to get target number of partitions
    uniqueh3 = sorted(list(ddf.index.unique().compute()))

    LOGGER.info('Repartitioning into %d partitions, based on parent cells', len(uniqueh3) + 1)
    LOGGER.info('Aggregating cell values where duplicates exist')

    ddf = ddf.repartition( # See "notes" on why divisions repeats last item https://docs.dask.org/en/stable/generated/dask.dataframe.DataFrame.repartition.html
        divisions=(uniqueh3 + [uniqueh3[-1]])
    ).map_partitions(
        # TODO make method configurable
        # lambda df: df.groupby(f'h3_{resolution}').mean().round(decimals=ROUND_DECIMALS)
        lambda df: df.groupby(f'h3_{resolution:02}').agg('mean').round(decimals=ROUND_DECIMALS)
        # lambda df: df.groupby(f'h3_{resolution}').agg(AGG_FUNC).round(decimals=ROUND_DECIMALS)
    ).to_parquet(
        output_path_aggregated, engine='pyarrow', compression=compression
    )

    LOGGER.info('Stage 2 (parent cell repartioning) and Stage 3 (aggregation) complete')

    return output_path_aggregated

@click.command(context_settings={'show_default': True})
@click.argument("raster_input", type=click.Path(exists=True))
@click.option('-r', '--resolution', required=True, type=click.Choice(list(map(str, range(MIN_H3, MAX_H3+1)))), help='H3 resolution to index')
@click.option('-c', '--compression', default='snappy', type=click.Choice(['snappy', 'gzip', 'zstd']), help='Name of the compression to use when writing to Parquet.')
@click.option('-t', '--threads', default=(mp.cpu_count() - 1), help='Number of threads to use when running in parallel')
@click.option('--warp_mem_limit', default=12000, type=int, help='Input raster may be warped to EPSG:4326 if it is not already in this CRS. This setting specifies the warp operation\'s memory limit in MB.')
@click.option('--resampling', default='average', type=click.Choice(Resampling._member_names_), help='Input raster may be warped to EPSG:4326 if it is not already in this CRS. This setting specifies the warp resampling algorithm.')
def raster2dggs(raster_input: Path, resolution: str, compression: str, threads: int, warp_mem_limit: int, resampling: str):
    
    warp_args : dict = {
        'resampling': Resampling._member_map_[resampling],
        'crs': crs.CRS.from_epsg(4326), # Input raster must be converted to WGS84 (4326) for H3 indexing
        'warp_mem_limit': warp_mem_limit
    }
    pq_intermediate = _initial_index(raster_input, int(resolution), compression, threads, warp_args)
    pq_output = _address_boundary_issues(pq_intermediate, int(resolution), compression)
    # remove stage intermediate output # TODO use temporary files, then make this function only call one function
    shutil.rmtree(pq_intermediate)
    return pq_output