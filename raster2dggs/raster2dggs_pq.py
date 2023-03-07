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
# import shutil
import tempfile
import threading
from typing import Tuple, Union

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
DEFAULT_NAME : str = 'value'

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
    subset = pd.pivot_table(subset, values=DEFAULT_NAME, index=['x','y'], columns=['band']).reset_index()
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

def _initial_index(raster_input: Union[Path, str], resolution: int, compression: str, aggfunc: str, decimals: int, max_workers: int, warp_args: dict)  -> tempfile.TemporaryDirectory:

    parent_resolution = _get_parent_res(resolution)
    LOGGER.info("Indexing at H3 resolution %d, parent resolution %d", resolution, parent_resolution)

    with tempfile.TemporaryDirectory() as tmpdir:
        LOGGER.info(f'Create temporary directory {tmpdir}')

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
                    default_name=DEFAULT_NAME,
                    band_as_variable=False # Using True will break VRT warping https://github.com/corteva/rioxarray/issues/644
                ).chunk(
                    **{'y':'auto', 'x':'auto'}
                )

                windows = [window for _, window in vrt.block_windows()][:10]
                LOGGER.debug('%d windows (the same number of partitions will be created)', len(windows))
            
                write_lock = threading.Lock()

                def process(window):
                    sdf = da.rio.isel_window(window)
                        
                    result = _h3func(sdf, resolution, parent_resolution, vrt.nodata, band_labels=band_names)
                    
                    with write_lock:
                        pq.write_to_dataset(result, root_path=tmpdir, compression=compression)

                with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                    executor.map(process, windows)

        LOGGER.info('Stage 1 (primary indexing) complete')
        return _address_boundary_issues(tmpdir, resolution, compression, aggfunc, decimals)

def _h3_parent_agg(df, resolution: int, aggfunc: str, decimals: int):
    if decimals > 0:
        return df.groupby(f'h3_{resolution:02}').agg(aggfunc).round(decimals)
    else:
        return df.groupby(f'h3_{resolution:02}').agg(aggfunc).round(decimals).astype(int)

def _address_boundary_issues(pq_input: tempfile.TemporaryDirectory, resolution: int, compression: str, aggfunc: str, decimals: int) -> Path:

    parent_resolution = _get_parent_res(resolution)

    # TODO use CLI for output location
    # TODO CLI option for overwrite, else error if already exists
    output_path_aggregated = Path(f'./tests/data/output/{resolution}/step-3')
    output_path_aggregated.mkdir(parents=True, exist_ok=True)

    LOGGER.info('Reading Stage 1 output and setting index for parent-based partitioning')
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
        _h3_parent_agg, resolution, aggfunc, decimals
    ).to_parquet(
        output_path_aggregated, engine='pyarrow', compression=compression
    )

    LOGGER.info('Stage 2 (parent cell repartioning) and Stage 3 (aggregation) complete')

    return output_path_aggregated

def _h3index(raster_input: Union[Path, str], resolution: int, compression: str, aggfunc: str, decimals: int, max_workers: int, warp_args: dict) -> Path:
    return _initial_index(raster_input, int(resolution), compression, aggfunc, decimals, max_workers, warp_args)

@click.command(context_settings={'show_default': True})
@click.argument("raster_input", type=str)
@click.option('-r', '--resolution', required=True, type=click.Choice(list(map(str, range(MIN_H3, MAX_H3+1)))), help='H3 resolution to index')
@click.option('-c', '--compression', default='snappy', type=click.Choice(['snappy', 'gzip', 'zstd']), help='Name of the compression to use when writing to Parquet.')
@click.option('-t', '--threads', default=(mp.cpu_count() - 1), help='Number of threads to use when running in parallel')
@click.option('-a', '--aggfunc', default='mean', type=click.Choice(['count', 'mean', 'sum', 'prod', 'std', 'var', 'min', 'max', 'median']), help='Numpy aggregate function to apply when aggregating cell values after DGGS indexing, in case of multiple pixels mapping to the same DGGS cell.')
@click.option('-d', '--decimals', default=1, type=int, help='Number of decimal places to round values when aggregating. Use 0 for integer output.')
@click.option('--warp_mem_limit', default=12000, type=int, help='Input raster may be warped to EPSG:4326 if it is not already in this CRS. This setting specifies the warp operation\'s memory limit in MB.')
@click.option('--resampling', default='average', type=click.Choice(Resampling._member_names_), help='Input raster may be warped to EPSG:4326 if it is not already in this CRS. This setting specifies the warp resampling algorithm.')
def raster2dggs(raster_input: Path, resolution: str, compression: str, threads: int, aggfunc: str, decimals: int, warp_mem_limit: int, resampling: str):
    
    _path = Path(raster_input)
    if not _path.exists() or _path.is_dir():
        LOGGER.warning(f'Input raster {raster_input} does not exist, or is a directory; assuming it is a remote URI')
    else:
        raster_input = _path
    warp_args : dict = {
        'resampling': Resampling._member_map_[resampling],
        'crs': crs.CRS.from_epsg(4326), # Input raster must be converted to WGS84 (4326) for H3 indexing
        'warp_mem_limit': warp_mem_limit
    }
    _h3index(raster_input, int(resolution), compression, aggfunc, decimals, threads, warp_args)
