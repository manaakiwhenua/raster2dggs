import os
import errno
import tempfile
import logging
import click_log

import pandas as pd

from typing import Union, Callable
from pathlib import Path
from rasterio import crs
from rasterio.enums import Resampling
from tqdm.dask import TqdmCallback
import dask.dataframe as dd

from urllib.parse import urlparse

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
        ),  # Input raster must be converted to WGS84 (4326) for H3 indexing
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


def get_parent_res(dggs: str, parent_res: Union[None, int], resolution: int) -> int:
    """
    Uses a parent resolution,
    OR,
    Given a target resolution, returns our recommended parent resolution.

    Used for intermediate re-partioning.
    """
    if dggs == "h3":
        return (
            int(parent_res)
            if parent_res is not None
            else max(const.MIN_H3, (resolution - const.DEFAULT_PARENT_OFFSET))
        )
    elif dggs == "rhp":
        return (
            int(parent_res)
            if parent_res is not None
            else max(const.MIN_RHP, (resolution - const.DEFAULT_PARENT_OFFSET))
        )
    else:
        raise RuntimeError(
            "Unknown dggs {dggs}) -  must be one of [ 'h3', 'rhp' ]".format(dggs=dggs)
        )


def address_boundary_issues(
    dggs: str,
    parent_groupby: Callable,
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
    parent_res = get_parent_res(dggs, parent_res, resolution)

    LOGGER.debug(
        f"Reading Stage 1 output ({pq_input}) and setting index for parent-based partitioning"
    )
    with TqdmCallback(desc="Reading window partitions"):
        # Set index as parent cell
        ddf = dd.read_parquet(pq_input).set_index(f"{dggs}_{parent_res:02}")

    with TqdmCallback(desc="Counting parents"):
        # Count parents, to get target number of partitions
        uniqueparents = sorted(list(ddf.index.unique().compute()))

    LOGGER.debug(
        "Repartitioning into %d partitions, based on parent cells",
        len(uniqueparents) + 1,
    )
    LOGGER.debug("Aggregating cell values where conflicts exist")

    with TqdmCallback(desc="Repartioning/aggregating"):
        ddf = (
            ddf.repartition(  # See "notes" on why divisions expects repetition of the last item https://docs.dask.org/en/stable/generated/dask.dataframe.DataFrame.repartition.html
                divisions=(uniqueparents + [uniqueparents[-1]])
            )
            .map_partitions(
                parent_groupby, resolution, kwargs["aggfunc"], kwargs["decimals"]
            )
            .to_parquet(
                output,
                overwrite=kwargs["overwrite"],
                engine="pyarrow",
                write_index=True,
                append=False,
                name_function=lambda i: f"{uniqueparents[i]}.parquet",
                compression=kwargs["compression"],
            )
        )

    LOGGER.debug(
        "Stage 2 (parent cell repartioning) and Stage 3 (aggregation) complete"
    )

    return output
