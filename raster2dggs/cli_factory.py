from dataclasses import dataclass
import textwrap
from typing import List, Optional

import click
import click_log
from rasterio.enums import Resampling

import raster2dggs.constants as const
import raster2dggs.common as common
from raster2dggs import __version__


@dataclass(frozen=True)
class DGGS_Spec:
    name: str  # command name and dggs key used by your indexers
    pretty: str  # for help text
    min_res: int
    max_res: int
    parent_help_default: str  # e.g. "resolution - 6"
    help: Optional[str] = None
    short_help: Optional[str] = None


SPECS: List[DGGS_Spec] = [
    DGGS_Spec("h3", "H3", const.MIN_H3, const.MAX_H3, "resolution - 6"),
    DGGS_Spec("rhp", "rHEALPix", const.MIN_RHP, const.MAX_RHP, "resolution - 6"),
    DGGS_Spec(
        "geohash", "Geohash", const.MIN_GEOHASH, const.MAX_GEOHASH, "resolution - 6"
    ),
    DGGS_Spec(
        "maidenhead",
        "Maidenhead",
        const.MIN_MAIDENHEAD,
        const.MAX_MAIDENHEAD,
        str(const.MIN_MAIDENHEAD),
    ),
    DGGS_Spec("s2", "S2", const.MIN_S2, const.MAX_S2, "resolution - 6"),
    DGGS_Spec("a5", "A5", const.MIN_A5, const.MAX_A5, "resolution - 6"),
    DGGS_Spec("isea9r", "ISEA9R", const.MIN_ISEA9R, const.MAX_ISEA9R, "resolution - 5"),
    DGGS_Spec("isea7h", "ISEA7H", const.MIN_ISEA7H, const.MAX_ISEA7H, "resolution - 6"),
]

HELP_TEMPLATE = """
    Ingest a raster image and index it to the {pretty} DGGS.

    RASTER_INPUT is the path to input raster data; prepend with protocol like s3:// or hdfs:// for remote data.
    OUTPUT_DIRECTORY should be a directory, not a file, as it will be the write location for an Apache Parquet data store, with partitions equivalent to parent cells of target cells at a fixed offset. However, this can also be remote (use the appropriate prefix, e.g. s3://). 
"""


def run_index(
    dggs: str,
    raster_input,
    output_directory,
    resolution: int,
    parent_res,
    band,
    upscale: int,
    compression: str,
    threads: int,
    aggfunc: str,
    decimals: int,
    overwrite: bool,
    warp_mem_limit: int,
    resampling: str,
    compact: bool,
    geo: str,
    tempdir,
):
    import tempfile as _tempfile  # local import to avoid any confusion

    _tempfile.tempdir = tempdir if tempdir is not None else _tempfile.tempdir

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
        geo,
    )

    common.initial_index(
        dggs,
        raster_input,
        output_directory,
        resolution,
        parent_res,
        warp_args,
        band,
        **kwargs,
    )


def make_command(spec: DGGS_Spec):
    help_text = textwrap.dedent(spec.help or HELP_TEMPLATE).format(pretty=spec.pretty)
    short_help = spec.short_help or f"Index raster data into the {spec.pretty} DGGS"

    @click.command(
        name=spec.name,
        context_settings={"show_default": True},
        help=help_text,
        short_help=short_help,
        # epilog="Any extra notes can go here."
    )
    @click_log.simple_verbosity_option(common.LOGGER)
    @click.argument("raster_input", type=click.Path(), nargs=1)
    @click.argument("output_directory", type=click.Path(), nargs=1)
    @click.option(
        "-r",
        "--resolution",
        required=True,
        type=click.IntRange(spec.min_res, spec.max_res),
        help=f"{spec.pretty} resolution to index",
    )
    @click.option(
        "-pr",
        "--parent_res",
        required=False,
        type=click.IntRange(spec.min_res, spec.max_res),
        help=f"{spec.pretty} parent resolution to index and aggregate to. Defaults to {spec.parent_help_default}",
    )
    @click.option(
        "-b",
        "--band",
        required=False,
        multiple=True,
        help="Band(s) to include in the output. Can specify multiple, e.g. `-b 1 -b 2 -b 4` for bands 1, 2, and 4 (all unspecified bands are ignored). If unused, all bands are included in the output (this is the default behaviour). Bands can be specified as numeric indices (1-based indexing) or string band labels (if present in the input), e.g. -b B02 -b B07 -b B12.",
    )
    @click.option(
        "-u",
        "--upscale",
        default=const.DEFAULTS["upscale"],
        type=int,
        help="Upscaling factor, used to upsample input data on the fly; useful when the raster resolution is lower than the target DGGS resolution. Default (1) applies no upscaling. The resampling method controls interpolation.",
    )
    @click.option(
        "-c",
        "--compression",
        default=const.DEFAULTS["compression"],
        type=str,
        help="Compression method to use for the output Parquet files. Options include 'snappy', 'gzip', 'brotli', 'lz4', 'zstd', etc. Use 'none' for no compression.",
    )
    @click.option(
        "-t",
        "--threads",
        default=const.DEFAULTS["threads"],
        help="Number of threads to use when running in parallel. The default is determined based dynamically as the total number of available cores, minus one.",
    )
    @click.option(
        "-a",
        "--aggfunc",
        default=const.DEFAULTS["aggfunc"],
        type=click.Choice(const.AGGFUNC_OPTIONS),
        help="Numpy aggregate function to apply when aggregating cell values after DGGS indexing, in case of multiple pixels mapping to the same DGGS cell.",
    )
    @click.option(
        "-d",
        "--decimals",
        default=const.DEFAULTS["decimals"],
        type=int,
        help="Number of decimal places to round values when aggregating. Use 0 for integer output.",
    )
    @click.option("-o", "--overwrite", is_flag=True)
    @click.option(
        "--warp_mem_limit",
        default=const.DEFAULTS["warp_mem_limit"],
        type=int,
        help="Input raster may be warped to EPSG:4326 if it is not already in this CRS. This setting specifies the warp operation's memory limit in MB.",
    )
    @click.option(
        "--resampling",
        default=const.DEFAULTS["resampling"],
        type=click.Choice(Resampling._member_names_),
        help="Input raster may be warped to EPSG:4326 if it is not already in this CRS. Or, if the upscale parameter is greater than 1, there is a need to resample. This setting specifies this resampling algorithm.",
    )
    @click.option(
        "-co",
        "--compact",
        is_flag=True,
        help="Compact the cells up to the parent resolution. Compaction is not applied for cells without identical values across all bands.",
    )
    @click.option(
        "-g",
        "--geo",
        default=const.DEFAULTS["geo"],
        type=click.Choice(const.GEOM_TYPES),
        help="Write output as a GeoParquet (v1.1.0) with either point or polygon geometry.",
    )
    @click.option(
        "--tempdir",
        default=const.DEFAULTS["tempdir"],
        type=click.Path(),
        help="Temporary data is created during the execution of this program. This parameter allows you to control where this data will be written.",
    )
    @click.version_option(version=__version__)
    def cmd(
        raster_input,
        output_directory,
        resolution,
        parent_res,
        band,
        upscale,
        compression,
        threads,
        aggfunc,
        decimals,
        overwrite,
        warp_mem_limit,
        resampling,
        compact,
        geo,
        tempdir,
    ):
        return run_index(
            spec.name,
            raster_input,
            output_directory,
            resolution,
            parent_res,
            band,
            upscale,
            compression,
            threads,
            aggfunc,
            decimals,
            overwrite,
            warp_mem_limit,
            resampling,
            compact,
            geo,
            tempdir,
        )

    return cmd
