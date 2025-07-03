import os
import errno
import tempfile
import logging
import threading
import rioxarray
import dask
import click_log

import rasterio as rio
import pandas as pd
import pyarrow.parquet as pq

from typing import Union, Callable
from pathlib import Path
from rasterio import crs
from rasterio.vrt import WarpedVRT
from rasterio.enums import Resampling
from tqdm import tqdm
from tqdm.dask import TqdmCallback
import dask.dataframe as dd
import xarray as xr

from concurrent.futures import ThreadPoolExecutor, as_completed

from urllib.parse import urlparse
from rasterio.warp import calculate_default_transform

import raster2dggs.constants as const

LOGGER = logging.getLogger(__name__)
click_log.basic_config(LOGGER)


class ParentResolutionException(Exception):
    pass


def check_resolutions(resolution: int, parent_res: int) -> None:
    if parent_res is not None and not int(parent_res) < int(resolution):
        raise ParentResolutionException(
            "Parent resolution ({pr}) must be less than target resolution ({r})".format(
                pr=parent_res, r=resolution
            )
        )


def resolve_input_path(raster_input: Union[str, Path]) -> Union[str, Path]:
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

    return raster_input


def assemble_warp_args(resampling: str, warp_mem_limit: int) -> dict:
    warp_args: dict = {
        "resampling": Resampling._member_map_[resampling],
        "crs": crs.CRS.from_epsg(
            4326
        ),  # Input raster must be converted to WGS84 (4326) for DGGS indexing
        "warp_mem_limit": warp_mem_limit,
    }

    return warp_args


def create_aggfunc(aggfunc: str) -> str:
    if aggfunc == "mode":
        logging.warning(
            "Mode aggregation: arbitrary behaviour: if there is more than one mode when aggregating, only the first value will be recorded."
        )
        aggfunc = lambda x: pd.Series.mode(x)[0]

    return aggfunc


def assemble_kwargs(
    upscale: int,
    compression: str,
    threads: int,
    aggfunc: str,
    decimals: int,
    warp_mem_limit: int,
    resampling: str,
    overwrite: bool,
) -> dict:
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

    return kwargs


def zero_padding(dggs: str) -> int:
    max_res_lookup = {
        "h3": const.MAX_H3,
        "rhp": const.MAX_RHP,
        "geohash": const.MAX_GEOHASH,
        "maidenhead": const.MAX_MAIDENHEAD,
        "s2": const.MAX_S2,
    }
    max_res = max_res_lookup.get(dggs)
    if max_res is None:
        raise ValueError(f"Unknown DGGS type: {dggs}")
    return len(str(max_res))


def get_parent_res(dggs: str, parent_res: Union[None, int], resolution: int) -> int:
    """
    Uses a parent resolution,
    OR,
    Given a target resolution, returns our recommended parent resolution.

    Used for intermediate re-partioning.
    """
    if not dggs in const.DEFAULT_DGGS_PARENT_RES.keys():
        raise RuntimeError(
            "Unknown dggs {dggs}) -  must be one of [ {options} ]".format(
                dggs=dggs, options=", ".join(const.DEFAULT_DGGS_PARENT_RES.keys())
            )
        )
    return (
        int(parent_res)
        if parent_res is not None
        else const.DEFAULT_DGGS_PARENT_RES[dggs](resolution)
    )


def address_boundary_issues(
    dggs: str,
    parent_groupby: Callable,
    compaction: Callable,
    pq_input: tempfile.TemporaryDirectory,
    output: Path,
    resolution: int,
    parent_res: int,
    **kwargs,
) -> Path:
    """
    After "stage 1" processing, there is a DGGS cell and band value/s for each pixel in the input image. Partitions are based
    on raster windows.

    This function will re-partition based on parent cell IDs at a fixed offset from the target resolution.

    Once re-partitioned on this basis, values are aggregated at the target resolution, to account for multiple pixels mapping
        to the same cell.

    This re-partitioning is necessary to address the issue of the same cell IDs being present in different partitions
        of the original (i.e. window-based) partitioning. Using the nested structure of the DGGS is an useful property
        to address this problem.
    """
    LOGGER.debug(
        f"Reading Stage 1 output ({pq_input}) and setting index for parent-based partitioning"
    )
    with TqdmCallback(desc="Reading window partitions"):
        # Set index as parent cell
        pad_width = zero_padding(dggs)
        index_col = f"{dggs}_{parent_res:0{pad_width}d}"
        ddf = dd.read_parquet(pq_input).set_index(index_col)

    with TqdmCallback(desc="Counting parents"):
        # Count parents, to get target number of partitions
        uniqueparents = sorted(list(ddf.index.unique().compute()))

    LOGGER.debug(
        "Repartitioning into %d partitions, based on parent cells",
        len(uniqueparents) + 1,
    )
    LOGGER.debug("Aggregating cell values where conflicts exist")

    with TqdmCallback(
        desc=f"Repartitioning/aggregating{'/compacting' if compaction else ''}"
    ):
        ddf = ddf.repartition(  # See "notes" on why divisions expects repetition of the last item https://docs.dask.org/en/stable/generated/dask.dataframe.DataFrame.repartition.html
            divisions=(uniqueparents + [uniqueparents[-1]])
        ).map_partitions(
            parent_groupby, resolution, kwargs["aggfunc"], kwargs["decimals"]
        )
        if compaction:
            ddf = ddf.map_partitions(compaction, resolution, parent_res)

        ddf.map_partitions(lambda df: df.sort_index()).to_parquet(
            output,
            overwrite=kwargs["overwrite"],
            engine="pyarrow",
            write_index=True,
            append=False,
            name_function=lambda i: f"{uniqueparents[i]}.parquet",
            compression=kwargs["compression"],
        )

    LOGGER.debug(
        "Stage 2 (parent cell repartitioning) and Stage 3 (aggregation) complete"
    )

    return output


def initial_index(
    dggs: str,
    dggsfunc: Callable,
    parent_groupby: Callable,
    compaction: Union[None, Callable],
    raster_input: Union[Path, str],
    output: Path,
    resolution: int,
    parent_res: Union[None, int],
    warp_args: dict,
    **kwargs,
) -> Path:
    """
    Responsible for opening the raster_input, and performing DGGS indexing per window of a WarpedVRT.

    A WarpedVRT is used to enforce reprojection to https://epsg.io/4326, which is used for all DGGS indexing.

    It also allows on-the-fly resampling of the input, which is useful if the target DGGS resolution exceeds the resolution
        of the input.

    This function passes a path to a temporary directory (which contains the output of this "stage 1" processing) to
        a secondary function that addresses issues at the boundaries of raster windows.
    """
    parent_res = get_parent_res(dggs, parent_res, resolution)
    LOGGER.info(
        "Indexing %s at %s resolution %d, parent resolution %d",
        raster_input,
        str(dggs),
        int(resolution),
        int(parent_res),
    )

    with tempfile.TemporaryDirectory() as tmpdir:
        LOGGER.debug(f"Create temporary directory {tmpdir}")

        # https://rasterio.readthedocs.io/en/latest/api/rasterio.warp.html#rasterio.warp.calculate_default_transform
        with rio.Env(CHECK_WITH_INVERT_PROJ=True):
            with rio.open(raster_input) as src:
                LOGGER.debug("Source CRS: %s", src.crs)
                # VRT used to avoid additional disk use given the potential for reprojection to 4326 prior to DGGS indexing
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
                        default_name=const.DEFAULT_NAME,
                    ).chunk(**{"y": "auto", "x": "auto"})

                    windows = [window for _, window in vrt.block_windows()]
                    LOGGER.debug(
                        "%d windows (the same number of partitions will be created)",
                        len(windows),
                    )

                    write_lock = threading.Lock()

                    def process(window):
                        sdf = da.rio.isel_window(window)

                        result = dggsfunc(
                            sdf,
                            resolution,
                            parent_res,
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
            return address_boundary_issues(
                dggs,
                parent_groupby,
                compaction,
                tmpdir,
                output,
                resolution,
                parent_res,
                **kwargs,
            )
