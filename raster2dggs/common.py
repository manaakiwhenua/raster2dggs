import os
import errno
import tempfile
import logging
import numpy as np
import threading
import rioxarray
import dask
import click_log

import rasterio as rio
import pandas as pd
import pyarrow.parquet as pq

from typing import Union, Optional, Sequence, Callable
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
import raster2dggs.indexerfactory as idxfactory

from raster2dggs.interfaces import RasterIndexer

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
        "resampling": Resampling[resampling],
        "crs": crs.CRS.from_epsg(
            4326
        ),  # Input raster must be converted to WGS84 (4326) for DGGS indexing
        "warp_mem_limit": warp_mem_limit,
    }

    return warp_args


def first_mode(x):
    m = pd.Series.mode(x, dropna=False)
    # Result is empty if all x is nan
    return m.iloc[0] if not m.empty else np.nan


def create_aggfunc(aggfunc: str) -> str:
    if aggfunc == "mode":
        logging.warning(
            "Mode aggregation: arbitrary behaviour: if there is more than one mode when aggregating, only the first value will be recorded."
        )
        aggfunc = first_mode

    return aggfunc


def assemble_kwargs(
    upscale: int,
    compression: str,
    threads: int,
    aggfunc: Union[str, Callable],
    decimals: int,
    warp_mem_limit: int,
    resampling: str,
    overwrite: bool,
    compact: bool,
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
        "compact": compact,
    }

    return kwargs


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
    indexer: RasterIndexer,
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
    # Set index as parent cell
    pad_width = const.zero_padding(indexer.dggs)
    index_col = f"{indexer.dggs}_{parent_res:0{pad_width}d}"
    ddf = dd.read_parquet(pq_input).set_index(index_col)

    with TqdmCallback(desc="Reading window partitions and counting parents"):
        # Count parents, to get target number of partitions
        uniqueparents = sorted(list(ddf.index.unique().compute()))

    LOGGER.debug(
        "Repartitioning into %d partitions, based on parent cells",
        len(uniqueparents) + 1,
    )
    LOGGER.debug("Aggregating cell values where conflicts exist")

    with TqdmCallback(
        desc=f"Repartitioning/aggregating{'/compacting' if kwargs['compact'] else ''}"
    ):
        ddf = ddf.repartition(  # See "notes" on why divisions expects repetition of the last item https://docs.dask.org/en/stable/generated/dask.dataframe.DataFrame.repartition.html
            divisions=(uniqueparents + [uniqueparents[-1]])
        ).map_partitions(
            indexer.parent_groupby, resolution, kwargs["aggfunc"], kwargs["decimals"]
        )
        if kwargs["compact"]:
            ddf = ddf.map_partitions(indexer.compaction, resolution, parent_res)

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
    raster_input: Union[Path, str],
    output: Path,
    resolution: int,
    parent_res: Union[None, int],
    warp_args: dict,
    bands: Optional[Sequence[Union[int, str]]] = None,
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
    indexer = idxfactory.indexer_instance(dggs)
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
            with rio.open(raster_input, mode="r", sharing=False) as src:
                LOGGER.debug("Source CRS: %s", src.crs)
                # VRT used to avoid additional disk use given the potential for reprojection to 4326 prior to DGGS indexing
                band_names = tuple(src.descriptions) if src.descriptions else tuple()
                count = src.count  # Bands
                labels_by_index = {
                    i: (
                        band_names[i - 1]
                        if i - 1 < len(band_names) and band_names[i - 1]
                        else f"band_{i}"
                    )
                    for i in range(1, count + 1)
                }
                if not bands:  # Covers None or empty tuple
                    selected_indices = list(range(1, count + 1))
                else:
                    if all(isinstance(b, int) or str(b).isdigit() for b in bands):
                        selected_indices = list(map(int, bands))
                    else:
                        name_to_index = {v: k for k, v in labels_by_index.items()}
                        try:
                            selected_indices = [name_to_index[str(b)] for b in bands]
                        except KeyError as e:
                            raise ValueError(
                                f"Requested band name not found: {e.args[0]}"
                            )
                    # Validate
                    for i in selected_indices:
                        if i < 1 or i > count:
                            raise ValueError(
                                f"Band index out of range: {i} (1..{count})"
                            )
                    # De-duplicate, preserving order
                    seen = set()
                    selected_indices = [
                        i for i in selected_indices if not (i in seen or seen.add(i))
                    ]

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
                    src,
                    src_crs=src.crs,
                    **warp_args,
                    **upsample_args,
                ) as vrt:
                    LOGGER.debug("VRT CRS: %s", vrt.crs)
                    da: xr.Dataset = rioxarray.open_rasterio(
                        vrt,
                        lock=dask.utils.SerializableLock(),
                        masked=True,
                        default_name=const.DEFAULT_NAME,
                    ).chunk(**{"y": "auto", "x": "auto"})

                    # Band selection
                    if "band" in da.dims and (len(selected_indices) != count):
                        if (
                            "band" in da.coords
                        ):  # rioxarray commonly exposes 1..N as band coords
                            da = da.sel(band=selected_indices)
                        else:
                            da = da.isel(band=[i - 1 for i in selected_indices])

                    windows = [window for _, window in vrt.block_windows()]
                    LOGGER.debug(
                        "%d windows (the same number of partitions will be created)",
                        len(windows),
                    )

                    selected_labels = tuple(
                        [labels_by_index[i] for i in selected_indices]
                    )
                    compression = kwargs["compression"]

                    def process(window):
                        sdf = da.rio.isel_window(window)

                        result = indexer.index_func(
                            sdf,
                            resolution,
                            parent_res,
                            vrt.nodata,
                            band_labels=selected_labels,
                        )

                        pq.write_to_dataset(
                            result,
                            root_path=tmpdir,
                            compression=compression,
                        )

                        return None

                    with ThreadPoolExecutor(
                        max_workers=kwargs["threads"]
                    ) as executor, tqdm(
                        total=len(windows), desc="Raster windows"
                    ) as pbar:
                        for _ in executor.map(process, windows, chunksize=1):
                            pbar.update(1)

            LOGGER.debug("Stage 1 (primary indexing) complete")
            return address_boundary_issues(
                indexer,
                tmpdir,
                output,
                resolution,
                parent_res,
                **kwargs,
            )
