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


import dask
import dask.dataframe as dd
import h3pandas # Necessary import despite lack of explicit use
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import rasterio as rio
from rasterio import crs
from rasterio.vrt import WarpedVRT
from rasterio.enums import Resampling
import rioxarray
import xarray as xr

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
LOGGER = logging.getLogger(__name__)

INPUT = Path('./tests/data/input/TestDEM.tif')
if not INPUT.exists():
    raise FileNotFoundError(INPUT)

MIN_H3, MAX_H3 = 0, 15
H3_RES : int = min(MAX_H3, 10)
H3_RES_PARENT : int = max(MIN_H3, H3_RES - 8)

OUTPUT_PATH_INDEXED = Path(f'./tests/data/output/{H3_RES}/step-1')
OUTPUT_PATH_INDEXED.mkdir(parents=True, exist_ok=False)

# OUTPUT_PATH_SORTED = Path('./tests/data/output/step-2')
# OUTPUT_PATH_SORTED.mkdir(parents=True, exist_ok=False)

OUTPUT_PATH_AGGREGATED = Path(f'./tests/data/output/{H3_RES}/step-3')
OUTPUT_PATH_AGGREGATED.mkdir(parents=True, exist_ok=False)

COMPRESSION : str = 'ZSTD'


LOGGER.info("Indexing at H3 resolution %d, parent resolution %d", H3_RES, H3_RES_PARENT)

CORES : int = mp.cpu_count() - 1

VALUE : str = 'value'

WARP_MEM_LIMIT : int = 12000

ROUND_DECIMALS : int = 1

REPROJECTION_RESAMPLING : Resampling = Resampling.average

LOGGER.debug("CPU cores: %s/%s", CORES, mp.cpu_count())

def h3func(sdf : xr.DataArray, nodata : Number = np.nan) -> pa.Table:
    '''
    Index a raster window to H3.
    Subsequent steps are necessary to resolve issues at the boundaries of windows.
    If windows are very small, or in strips rather than blocks, processing may be slower
    than necessary and the recommendation is to write different windows in the source raster. 
    '''
    subset : pd.DataFrame = sdf.to_dataframe(name=VALUE).reset_index().drop('spatial_ref', axis=1).dropna()
    subset = subset[subset.value != nodata]
    # Primary H3 index
    h3index = subset.h3.geo_to_h3(H3_RES, lat_col='y', lng_col='x').drop(columns= ['x','y'])
    # Secondary (parent) H3 index, used later for partitioning
    h3index = h3index.h3.h3_to_parent(H3_RES_PARENT).reset_index()
    return pa.Table.from_pandas(h3index)

with rio.open(INPUT) as src:
    LOGGER.debug('Source CRS: %s', src.crs)
    
    vrt_args : dict = {
        'resampling': REPROJECTION_RESAMPLING,
        'crs': crs.CRS.from_epsg(4326), # Input raster must be converted to WGS84 (4326) for H3 indexing
        'warp_mem_limit': WARP_MEM_LIMIT
    } # VRT used to avoid additional disk use given the need for reprojection
    chunk_args : dict = {
        'y':'auto', 'x':'auto', 'band': 1
    }
    
    with WarpedVRT(src, src_crs=src.crs, **vrt_args) as vrt:
        LOGGER.debug('VRT CRS: %s', vrt.crs)
        da : xr.Dataset = rioxarray.open_rasterio(vrt).chunk(**chunk_args).squeeze().drop_vars(["spatial_ref", "band"])
        da.name = "data" # TODO necessary ?
        
        windows = [window for _, window in vrt.block_windows()]
        LOGGER.debug('%d windows (the same number of partitions will be created)', len(windows))
        
        read_lock = threading.Lock()
        write_lock = threading.Lock()

        def process(window):
            with read_lock:
                sdf = da.rio.isel_window(window)
                
            result = h3func(sdf, vrt.nodata)
            
            with write_lock:
                pq.write_to_dataset(result, root_path=OUTPUT_PATH_INDEXED, compression=COMPRESSION)

        with concurrent.futures.ThreadPoolExecutor(max_workers=CORES) as executor:
            executor.map(process, windows)

LOGGER.info('Stage 1 (primary indexing) complete')

ddf = dd.read_parquet(
    OUTPUT_PATH_INDEXED
).set_index( # Set index as parent cell
    f'h3_{H3_RES_PARENT:02}'
)

# Count parents, to get target number of partitions
uniqueh3 = sorted(list(ddf.index.unique().compute()))

LOGGER.info('Repartitioning into %d partitions, based on parent cells', len(uniqueh3) + 1)
LOGGER.info('Aggregating cell values where duplicates exist')

ddf = ddf.repartition( # See "notes" on why divisions repeats last item https://docs.dask.org/en/stable/generated/dask.dataframe.DataFrame.repartition.html
    divisions=(uniqueh3 + [uniqueh3[-1]])
).map_partitions(
    # TODO make method configurable
    lambda df: df.groupby(f'h3_{H3_RES}').mean().round(decimals=ROUND_DECIMALS)
).to_parquet(
    OUTPUT_PATH_AGGREGATED, engine='pyarrow', compression=COMPRESSION
)

LOGGER.info('Stage 2 (parent cell repartioning) and Stage 3 (aggregation) complete')

# remove stage 1 temporary output
shutil.rmtree(OUTPUT_PATH_INDEXED)