import click
import click_log
import tempfile

from pathlib import Path
from typing import Optional, Sequence, Union
from rasterio.enums import Resampling

import raster2dggs.constants as const
import raster2dggs.common as common
from raster2dggs import __version__


@click.command(context_settings={"show_default": True})
@click_log.simple_verbosity_option(common.LOGGER)
@click.argument("raster_input", type=click.Path(), nargs=1)
@click.argument("output_directory", type=click.Path(), nargs=1)
@click.option(
    "-r",
    "--resolution",
    required=True,
    type=click.Choice(list(map(str, range(const.MIN_GEOHASH, const.MAX_GEOHASH + 1)))),
    help=const.OPTION_HELP['resolution']('Geohash'),
)
@click.option(
    "-pr",
    "--parent_res",
    required=False,
    type=click.Choice(list(map(str, range(const.MIN_GEOHASH, const.MAX_GEOHASH + 1)))),
    help=const.OPTION_HELP['parent_res']('Geohash', 'resolution - 6'),
)
@click.option(
    "-b",
    "--band",
    required=False,
    multiple=True,
    help=const.OPTION_HELP['band'],
)
@click.option(
    "-u",
    "--upscale",
    default=const.DEFAULTS["upscale"],
    type=int,
    help=const.OPTION_HELP['upscale'],
)
@click.option(
    "-c",
    "--compression",
    default=const.DEFAULTS["compression"],
    type=str,
    help=const.OPTION_HELP['compression'],
)
@click.option(
    "-t",
    "--threads",
    default=const.DEFAULTS["threads"],
    help=const.OPTION_HELP['threads'],
)
@click.option(
    "-a",
    "--aggfunc",
    default=const.DEFAULTS["aggfunc"],
    type=click.Choice(
        const.AGGFUNC_OPTIONS
    ),
    help=const.OPTION_HELP['aggfunc'],
)
@click.option(
    "-d",
    "--decimals",
    default=const.DEFAULTS["decimals"],
    type=int,
    help=const.OPTION_HELP['decimals'],
)
@click.option("-o", "--overwrite", is_flag=True)
@click.option(
    "--warp_mem_limit",
    default=const.DEFAULTS["warp_mem_limit"],
    type=int,
    help=const.OPTION_HELP['warp_mem_limit'],
)
@click.option(
    "--resampling",
    default=const.DEFAULTS["resampling"],
    type=click.Choice(Resampling._member_names_),
    help=const.OPTION_HELP['resampling'],
)
@click.option(
    "-co",
    "--compact",
    is_flag=True,
    help=const.OPTION_HELP['compact'],
)
@click.option(
    "--tempdir",
    default=const.DEFAULTS["tempdir"],
    type=click.Path(),
    help=const.OPTION_HELP['tempdir'],
)
@click.version_option(version=__version__)
def geohash(
    raster_input: Union[str, Path],
    output_directory: Union[str, Path],
    resolution: str,
    parent_res: str,
    band: Optional[Sequence[Union[int, str]]],
    upscale: int,
    compression: str,
    threads: int,
    aggfunc: str,
    decimals: int,
    overwrite: bool,
    warp_mem_limit: int,
    resampling: str,
    compact: bool,
    tempdir: Union[str, Path],
):
    """
    Ingest a raster image and index it using the Geohash geocode system.

    RASTER_INPUT is the path to input raster data; prepend with protocol like s3:// or hdfs:// for remote data.
    OUTPUT_DIRECTORY should be a directory, not a file, as it will be the write location for an Apache Parquet data store, with partitions equivalent to parent cells of target cells at a fixed offset. However, this can also be remote (use the appropriate prefix, e.g. s3://).
    """
    tempfile.tempdir = tempdir if tempdir is not None else tempfile.tempdir

    common.check_resolutions(resolution, parent_res)

    raster_input = common.resolve_input_path(raster_input)
    warp_args = common.assemble_warp_args(resampling, warp_mem_limit)
    aggfunc = common.create_aggfunc(aggfunc)
    kwargs = common.assemble_kwargs(
        upscale,
        compression,
        threads,
        aggfunc,
        decimals,
        warp_mem_limit,
        resampling,
        overwrite,
        compact,
    )

    common.initial_index(
        "geohash",
        raster_input,
        output_directory,
        int(resolution),
        parent_res,
        warp_args,
        band,
        **kwargs,
    )
