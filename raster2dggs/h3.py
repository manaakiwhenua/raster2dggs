from concurrent.futures import ThreadPoolExecutor, as_completed
import errno
import logging
import os
import multiprocessing
from numbers import Number
import numpy as np
from pathlib import Path
import tempfile
import threading
from typing import Tuple, Union
from urllib.parse import urlparse

import click
import click_log
import dask
import dask.dataframe as dd
import h3pandas  # Necessary import despite lack of explicit use
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import rasterio as rio
from rasterio import crs
from rasterio.enums import Resampling
from rasterio.vrt import WarpedVRT
from rasterio.warp import calculate_default_transform
import rioxarray
from tqdm import tqdm
from tqdm.dask import TqdmCallback
import xarray as xr

LOGGER = logging.getLogger(__name__)
click_log.basic_config(LOGGER)

MIN_H3, MAX_H3 = 0, 15
DEFAULT_NAME: str = "value"

DEFAULTS = {
    "upscale": 1,
    "compression": "snappy",
    "threads": (multiprocessing.cpu_count() - 1),
    "aggfunc": "mean",
    "decimals": 1,
    "warp_mem_limit": 12000,
    "resampling": "average",
}


def _get_parent_res(resolution: int) -> int:
    """
    Given a target resolution, returns our recommended parent resolution.

    Used for intermediate re-partioning.
    """
    return max(MIN_H3, resolution - 8)


def _h3func(
    sdf: xr.DataArray,
    resolution: int,
    parent_resolution: int,
    nodata: Number = np.nan,
    band_labels: Tuple[str] = None,
) -> pa.Table:
    """
    Index a raster window to H3.
    Subsequent steps are necessary to resolve issues at the boundaries of windows.
    If windows are very small, or in strips rather than blocks, processing may be slower
    than necessary and the recommendation is to write different windows in the source raster.
    """
    sdf: pd.DataFrame = sdf.to_dataframe().drop(columns=["spatial_ref"]).reset_index()
    subset: pd.DataFrame = sdf.dropna()
    subset = subset[subset.value != nodata]
    subset = pd.pivot_table(
        subset, values=DEFAULT_NAME, index=["x", "y"], columns=["band"]
    ).reset_index()
    # Primary H3 index
    h3index = subset.h3.geo_to_h3(resolution, lat_col="y", lng_col="x").drop(
        columns=["x", "y"]
    )
    # Secondary (parent) H3 index, used later for partitioning
    h3index = h3index.h3.h3_to_parent(parent_resolution).reset_index()
    # Renaming columns to actual band labels
    bands = sdf["band"].unique()
    band_names = dict(zip(bands, map(lambda i: band_labels[i - 1], bands)))
    for k, v in band_names.items():
        if band_names[k] is None:
            band_names[k] = str(bands[k - 1])
        else:
            band_names = band_names
    h3index = h3index.rename(columns=band_names)
    return pa.Table.from_pandas(h3index)


def _initial_index(
    raster_input: Union[Path, str],
    output: Path,
    resolution: int,
    warp_args: dict,
    **kwargs,
) -> Path:
    """
    Responsible for opening the raster_input, and performing H3 indexing per window of a WarpedVRT.

    A WarpedVRT is used to enforce reprojection to https://epsg.io/4326, which is required for H3 indexing.

    It also allows on-the-fly resampling of the input, which is useful if the target H3 resolution exceeds the resolution
        of the input.

    This function passes a path to a temporary directory (which contains the output of this "stage 1" processing) to
        a secondary function that addresses issues at the boundaries of raster windows.
    """
    parent_resolution = _get_parent_res(resolution)
    LOGGER.info(
        "Indexing %s at H3 resolution %d, parent resolution %d",
        raster_input,
        resolution,
        parent_resolution,
    )

    with tempfile.TemporaryDirectory() as tmpdir:
        LOGGER.debug(f"Create temporary directory {tmpdir}")

        # https://rasterio.readthedocs.io/en/latest/api/rasterio.warp.html#rasterio.warp.calculate_default_transform
        with rio.Env(CHECK_WITH_INVERT_PROJ=True):
            with rio.open(raster_input) as src:
                LOGGER.debug("Source CRS: %s", src.crs)
                # VRT used to avoid additional disk use given the potential for reprojection to 4326 prior to H3 indexing
                band_names = src.descriptions

                upscale_factor = kwargs["upscale"]
                if upscale_factor > 1:
                    dst_crs = warp_args["crs"]
                    transform, width, height = calculate_default_transform(
                        src.crs,
                        dst_crs,
                        src.width,
                        src.height,
                        *src.bounds,
                        dst_width=src.width * upscale_factor,
                        dst_height=src.height * upscale_factor,
                    )
                    upsample_args = dict(
                        {"transform": transform, "width": width, "height": height}
                    )
                    LOGGER.debug(upsample_args)
                else:
                    upsample_args = dict({})

                with WarpedVRT(
                    src, src_crs=src.crs, **warp_args, **upsample_args
                ) as vrt:
                    LOGGER.debug("VRT CRS: %s", vrt.crs)
                    da: xr.Dataset = rioxarray.open_rasterio(
                        vrt,
                        lock=dask.utils.SerializableLock(),
                        masked=True,
                        default_name=DEFAULT_NAME,
                    ).chunk(**{"y": "auto", "x": "auto"})

                    windows = [window for _, window in vrt.block_windows()]
                    LOGGER.debug(
                        "%d windows (the same number of partitions will be created)",
                        len(windows),
                    )

                    write_lock = threading.Lock()

                    def process(window):
                        sdf = da.rio.isel_window(window)

                        result = _h3func(
                            sdf,
                            resolution,
                            parent_resolution,
                            vrt.nodata,
                            band_labels=band_names,
                        )

                        with write_lock:
                            pq.write_to_dataset(
                                result,
                                root_path=tmpdir,
                                compression=kwargs["compression"],
                            )

                        return None

                    with tqdm(total=len(windows), desc="Raster windows") as pbar:
                        with ThreadPoolExecutor(
                            max_workers=kwargs["threads"]
                        ) as executor:
                            futures = [
                                executor.submit(process, window) for window in windows
                            ]
                            for future in as_completed(futures):
                                result = future.result()
                                pbar.update(1)

            LOGGER.debug("Stage 1 (primary indexing) complete")
            return _address_boundary_issues(tmpdir, output, resolution, **kwargs)


def _h3_parent_groupby(df, resolution: int, aggfunc: str, decimals: int):
    """
    Function for aggregating the h3 resolution values per parent partition. Each partition will be run through with a
    pandas .groupby function. This step is to ensure there are no duplicate h3 values, which will happen when indexing a
    high resolution raster at a coarser h3 resolution.
    """
    if decimals > 0:
        return df.groupby(f"h3_{resolution:02}").agg(aggfunc).round(decimals)
    else:
        return (
            df.groupby(f"h3_{resolution:02}").agg(aggfunc).round(decimals).astype(int)
        )


def _address_boundary_issues(
    pq_input: tempfile.TemporaryDirectory, output: Path, resolution: int, **kwargs
) -> Path:
    """
    After "stage 1" processing, there is an H3 cell and band value/s for each pixel in the input image. Partitions are based
    on raster windows.

    This function will re-partition based on H3 parent cell IDs at a fixed offset from the target resolution.

    Once re-partitioned on this basis, values are aggregated at the target resolution, to account for multiple pixels mapping
        to the same H3 cell.

    This re-partitioning is necessary to address the issue of the same H3 cell IDs being present in different partitions
        of the original (i.e. window-based) partitioning. Using the nested structure of the DGGS is an useful property
        to address this problem.
    """
    parent_resolution = _get_parent_res(resolution)

    LOGGER.debug(
        f"Reading Stage 1 output ({pq_input}) and setting index for parent-based partitioning"
    )
    with TqdmCallback(desc="Reading window partitions"):
        ddf = dd.read_parquet(pq_input).set_index(  # Set index as parent cell
            f"h3_{parent_resolution:02}"
            if parent_resolution > 0
            else "h3_parent"  # https://github.com/DahnJ/H3-Pandas/issues/15
        )

    with TqdmCallback(desc="Counting parents"):
        # Count parents, to get target number of partitions
        uniqueh3 = sorted(list(ddf.index.unique().compute()))

    LOGGER.debug(
        "Repartitioning into %d partitions, based on parent cells", len(uniqueh3) + 1
    )
    LOGGER.debug("Aggregating cell values where conflicts exist")

    with TqdmCallback(desc="Repartioning/aggregating"):
        ddf = (
            ddf.repartition(  # See "notes" on why divisions expects repetition of the last item https://docs.dask.org/en/stable/generated/dask.dataframe.DataFrame.repartition.html
                divisions=(uniqueh3 + [uniqueh3[-1]])
            )
            .map_partitions(
                _h3_parent_groupby, resolution, kwargs["aggfunc"], kwargs["decimals"]
            )
            .to_parquet(
                output,
                overwrite=kwargs["overwrite"],
                engine="pyarrow",
                write_index=True,
                append=False,
                compression=kwargs["compression"],
            )
        )

    LOGGER.debug(
        "Stage 2 (parent cell repartioning) and Stage 3 (aggregation) complete"
    )

    return output


@click.command(context_settings={"show_default": True})
@click_log.simple_verbosity_option(LOGGER)
@click.argument("raster_input", type=click.Path(), nargs=1)
@click.argument("output_directory", type=click.Path(), nargs=1)
@click.option(
    "-r",
    "--resolution",
    required=True,
    type=click.Choice(list(map(str, range(MIN_H3, MAX_H3 + 1)))),
    help="H3 resolution to index",
)
@click.option(
    "-u",
    "--upscale",
    default=DEFAULTS["upscale"],
    type=int,
    help="Upscaling factor, used to upsample input data on the fly; useful when the raster resolution is lower than the target DGGS resolution. Default (1) applies no upscaling. The resampling method controls interpolation.",
)
@click.option(
    "-c",
    "--compression",
    default=DEFAULTS["compression"],
    type=click.Choice(["snappy", "gzip", "zstd"]),
    help="Name of the compression to use when writing to Parquet.",
)
@click.option(
    "-t",
    "--threads",
    default=DEFAULTS["threads"],
    help="Number of threads to use when running in parallel. The default is determined based dynamically as the total number of available cores, minus one.",
)
@click.option(
    "-a",
    "--aggfunc",
    default=DEFAULTS["aggfunc"],
    type=click.Choice(
        ["count", "mean", "sum", "prod", "std", "var", "min", "max", "median"]
    ),
    help="Numpy aggregate function to apply when aggregating cell values after DGGS indexing, in case of multiple pixels mapping to the same DGGS cell.",
)
@click.option(
    "-d",
    "--decimals",
    default=DEFAULTS["decimals"],
    type=int,
    help="Number of decimal places to round values when aggregating. Use 0 for integer output.",
)
@click.option("-o", "--overwrite", is_flag=True)
@click.option(
    "--warp_mem_limit",
    default=DEFAULTS["warp_mem_limit"],
    type=int,
    help="Input raster may be warped to EPSG:4326 if it is not already in this CRS. This setting specifies the warp operation's memory limit in MB.",
)
@click.option(
    "--resampling",
    default=DEFAULTS["resampling"],
    type=click.Choice(Resampling._member_names_),
    help="Input raster may be warped to EPSG:4326 if it is not already in this CRS. Or, if the upscale parameter is greater than 1, there is a need to resample. This setting specifies this resampling algorithm.",
)
def h3(
    raster_input: Union[str, Path],
    output_directory: Union[str, Path],
    resolution: str,
    upscale: int,
    compression: str,
    threads: int,
    aggfunc: str,
    decimals: int,
    overwrite: bool,
    warp_mem_limit: int,
    resampling: str,
):
    """
    Ingest a raster image and index it to the H3 DGGS.

    RASTER_INPUT is the path to input raster data; prepend with protocol like s3:// or hdfs:// for remote data.
    OUTPUT_DIRECTORY should be a directory, not a file, as it will be the write location for an Apache Parquet data store, with partitions equivalent to parent cells of target cells at a fixed offset. However, this can also be remote (use the appropriate prefix, e.g. s3://).
    """
    if not Path(raster_input).exists():
        if not urlparse(raster_input).scheme:
            LOGGER.warning(
                f"Input raster {raster_input} does not exist, and is not recognised as a remote URI"
            )
            raise FileNotFoundError(
                errno.ENOENT, os.strerror(errno.ENOENT), raster_input
            )
        # Quacks like a path to remote data
        raster_input = str(raster_input)
    else:
        raster_input = Path(raster_input)
    warp_args: dict = {
        "resampling": Resampling._member_map_[resampling],
        "crs": crs.CRS.from_epsg(
            4326
        ),  # Input raster must be converted to WGS84 (4326) for H3 indexing
        "warp_mem_limit": warp_mem_limit,
    }
    kwargs = {
        "upscale": upscale,
        "compression": compression,
        "threads": threads,
        "aggfunc": aggfunc,
        "decimals": decimals,
        "warp_mem_limit": warp_mem_limit,
        "resampling": resampling,
        "overwrite": overwrite,
    }
    _initial_index(raster_input, output_directory, int(resolution), warp_args, **kwargs)
