from dataclasses import dataclass
import textwrap
import tempfile
from typing import List, Optional

import click
import click_log

import raster2dggs.constants as const
import raster2dggs.common as common
from raster2dggs import __version__


class AggFuncListParamType(click.ParamType):
    """Accepts one or more comma-separated aggregation function names."""

    name = "AGGFUNC"

    def convert(self, value, param, ctx):
        if isinstance(value, tuple):
            return value
        parts = tuple(v.strip() for v in str(value).split(","))
        for p in parts:
            if p not in const.AGGFUNC_OPTIONS:
                self.fail(
                    f"'{p}' is not a valid aggregation function. "
                    f"Choose from: {', '.join(const.AGGFUNC_OPTIONS)}",
                    param,
                    ctx,
                )
        return parts

    def get_metavar(self, param, ctx=None):
        return "AGGFUNC[,AGGFUNC...]"


class DecimalsParamType(click.ParamType):
    """Accepts a non-negative integer or 'none' (meaning: do not round)."""

    name = "DECIMALS"

    def convert(self, value, param, ctx):
        if value is None:
            return None
        if isinstance(value, int):
            return value
        if isinstance(value, str) and value.lower() == "none":
            return None
        try:
            i = int(value)
            if i < 0:
                self.fail(f"{i}: decimals must be >= 0", param, ctx)
            return i
        except (ValueError, TypeError):
            self.fail(
                f"'{value}': expected a non-negative integer or 'none'", param, ctx
            )

    def get_metavar(self, param, ctx=None):
        return "INTEGER|none"


class ResolutionParamType(click.ParamType):
    """Accepts an integer resolution in [min_res, max_res] or a named auto-detection mode."""

    name = "RESOLUTION"

    def __init__(self, min_res: int, max_res: int):
        self.min_res = min_res
        self.max_res = max_res

    def convert(self, value, param, ctx):
        if isinstance(value, int):
            return value
        if value in const.ResolutionMode:
            return const.ResolutionMode(value)
        try:
            int_val = int(value)
        except (ValueError, TypeError):
            self.fail(
                f"'{value}': expected an integer in [{self.min_res}, {self.max_res}] "
                f"or one of: {'|'.join(const.ResolutionMode)}",
                param,
                ctx,
            )
        if not (self.min_res <= int_val <= self.max_res):
            self.fail(
                f"{int_val} is outside the valid range [{self.min_res}, {self.max_res}]",
                param,
                ctx,
            )
        return int_val

    def get_metavar(self, param, ctx=None):
        return f"[{self.min_res}-{self.max_res}|{'|'.join(const.ResolutionMode)}]"


@dataclass(frozen=True)
class DGGS_Spec:
    name: str  # command name and dggs key used by indexers
    pretty: str  # for help text
    min_res: int
    max_res: int
    default_parent_offset: int  # Chosen so as to be closest to containing 64K sub-zones
    help: Optional[str] = None
    short_help: Optional[str] = None


SPECS: List[DGGS_Spec] = [
    DGGS_Spec("h3", "H3", const.MIN_H3, const.MAX_H3, 6),
    DGGS_Spec("rhp", "rHEALPix", const.MIN_RHP, const.MAX_RHP, 5),
    DGGS_Spec("geohash", "Geohash", const.MIN_GEOHASH, const.MAX_GEOHASH, 4),
    DGGS_Spec(
        "maidenhead",
        "Maidenhead",
        const.MIN_MAIDENHEAD,
        const.MAX_MAIDENHEAD,
        3,
    ),
    DGGS_Spec("s2", "S2", const.MIN_S2, const.MAX_S2, 8),
    DGGS_Spec("a5", "A5", const.MIN_A5, const.MAX_A5, 8),
    DGGS_Spec("isea4r", "ISEA4R", const.MIN_ISEA4R, const.MAX_ISEA4R, 8),
    DGGS_Spec("isea9r", "ISEA9R", const.MIN_ISEA9R, const.MAX_ISEA9R, 5),
    DGGS_Spec("isea3h", "ISEA3H", const.MIN_ISEA3H, const.MAX_ISEA3H, 10),
    DGGS_Spec("isea7h", "ISEA7H", const.MIN_ISEA7H, const.MAX_ISEA7H, 6),
    # DGGS_Spec("isea7h_z7", "ISEA7H_Z7", const.MIN_ISEA7H_Z7, const.MAX_ISEA7H_Z7, 6),
    DGGS_Spec("ivea4r", "IVEA4R", const.MIN_IVEA4R, const.MAX_IVEA4R, 8),
    DGGS_Spec("ivea9r", "IVEA9R", const.MIN_IVEA9R, const.MAX_IVEA9R, 5),
    DGGS_Spec("ivea3h", "IVEA3H", const.MIN_IVEA3H, const.MAX_IVEA3H, 10),
    DGGS_Spec("ivea7h", "IVEA7H", const.MIN_IVEA7H, const.MAX_IVEA7H, 6),
    # DGGS_Spec("ivea7h_z7", "IVEA7H_Z7", const.MIN_IVEA7H_Z7, const.MAX_IVEA7H_Z7, 6),
    DGGS_Spec("rtea4r", "RTEA9R", const.MIN_RTEA4R, const.MAX_RTEA4R, 8),
    DGGS_Spec("rtea9r", "RTEA9R", const.MIN_RTEA9R, const.MAX_RTEA9R, 5),
    DGGS_Spec("rtea7h", "RTEA7H", const.MIN_RTEA7H, const.MAX_RTEA7H, 6),
    # DGGS_Spec("rtea7h_z7", "RTEA7H_Z7", const.MIN_RTEA7H_Z7, const.MAX_RTEA7H_Z7, 6),
    DGGS_Spec("healpix", "HEALPix", const.MIN_HEALPIX, const.MAX_HEALPIX, 5),
    DGGS_Spec(
        "rhealpix", "rHEALPix", const.MIN_RHEALPIX, const.MAX_RHEALPIX, 5
    ),  # Prefer rhp
]
# NB use 5 for IS/VEA9R, and 10 for IS/VEA3H, and 8 for GNOSIS --- corresponds to ~64K sub-zones

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
    parent_res: int,
    band,
    nodata_policy: str,
    emit_nodata_value: Optional[float],
    compression: str,
    threads: int,
    agg,
    decimals,
    overwrite: bool,
    compact: bool,
    geo: str,
    tempdir,
    semantics: str,
    transfer: str,
    out: str,
):
    tempfile.tempdir = tempdir if tempdir is not None else tempfile.tempdir

    common.check_resolutions(resolution, parent_res)

    raster_input = common.resolve_input_path(raster_input)
    agg = common.create_aggfuncs(agg, decimals)
    kwargs = common.assemble_kwargs(
        compression,
        threads,
        agg,
        decimals,
        overwrite,
        compact,
        geo,
        semantics,
        transfer,
        out,
    )

    common.initial_index(
        dggs,
        raster_input,
        output_directory,
        resolution,
        parent_res,
        band,
        nodata_policy,
        emit_nodata_value,
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
        type=ResolutionParamType(spec.min_res, spec.max_res),
        help=(
            f"{spec.pretty} resolution to index. "
            f"Accepts an integer in [{spec.min_res}, {spec.max_res}] or an auto-detection "
            f"mode: 'smaller-than-pixel' (first resolution finer than a pixel), "
            f"'larger-than-pixel' (last resolution coarser than a pixel), or "
            f"'min-diff' (resolution closest to pixel size)."
        ),
    )
    @click.option(
        "-pr",
        "--parent_res",
        required=False,
        type=click.IntRange(spec.min_res, spec.max_res),
        help=f"{spec.pretty} parent resolution to index and aggregate to. Defaults to max({spec.min_res}, resolution - {spec.default_parent_offset})",
    )
    @click.option(
        "-b",
        "--band",
        required=False,
        multiple=True,
        help="Band(s) to include in the output. Can specify multiple, e.g. `-b 1 -b 2 -b 4` for bands 1, 2, and 4 (all unspecified bands are ignored). If unused, all bands are included in the output (this is the default behaviour). Bands can be specified as numeric indices (1-based indexing) or string band labels (if present in the input), e.g. -b B02 -b B07 -b B12.",
    )
    @click.option(
        "--nodata_policy",
        type=click.Choice(const.NODATA_POLICY_OPTIONS, case_sensitive=False),
        default=const.DEFAULTS["nodata_policy"],
        show_default=True,
        help=(
            "'omit' excludes nodata cells from output (default). "
            "'emit' includes them, writing the source raster nodata value (or --emit_nodata_value if set). "
            "Note: non-NaN emitted values participate in cell aggregation (see -a/--agg); "
            "if this is undesired, ensure your source nodata is NaN or override with --emit_nodata_value."
        ),
    )
    @click.option(
        "--emit_nodata_value",
        default=None,
        type=float,
        metavar="NUMBER",
        help=(
            "Override the value written for nodata cells when --nodata_policy=emit. "
            "If omitted, the source raster nodata value is used (NaN if none is defined). "
            "Pass 'nan' to explicitly emit NaN. "
            "Coerced to the output dtype. "
            "Note: non-NaN values participate in cell aggregation (see -a/--agg)."
        ),
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
        "--agg",
        default=const.DEFAULTS["aggfunc"],
        type=AggFuncListParamType(),
        show_default=True,
        help=(
            "Aggregation function(s) applied when multiple raster pixels map to the same DGGS cell. "
            f"Options: {', '.join(const.AGGFUNC_OPTIONS)}. "
            "Comma-separate multiple names (e.g. min,max) to produce a struct column per band."
        ),
    )
    @click.option(
        "--semantics",
        default=const.Semantics.POINT_CENTER_STRICT.value,
        type=click.Choice([s.value for s in const.Semantics]),
        show_default=True,
        help="What a raster cell value means (determines valid transfer operators).",
    )
    @click.option(
        "--transfer",
        default=const.Transfer.ASSIGN_CENTERS.value,
        type=click.Choice([t.value for t in const.Transfer]),
        show_default=True,
        help="How values are mapped from raster pixels to DGGS cells.",
    )
    @click.option(
        "--out",
        default=const.OutputSchema.VALUE.value,
        type=click.Choice([o.value for o in const.OutputSchema]),
        show_default=True,
        help="Output schema: scalar value, class fractions, histogram, or sorted sample list. 'list' collects all contributing pixel values per cell in ascending order (deterministic); use -d/--decimals to control precision.",
    )
    @click.option(
        "-d",
        "--decimals",
        default=const.DEFAULTS["decimals"],
        type=DecimalsParamType(),
        help="Decimal places to round output values. Use 0 for integer output, 'none' to disable rounding.",
    )
    @click.option("-o", "--overwrite", is_flag=True)
    @click.option(
        "-co",
        "--compact",
        is_flag=True,
        help="Compact the cells up to the parent resolution. Compaction is only applied where all sibling cells share identical values in every output column.",
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
        nodata_policy,
        emit_nodata_value,
        compression,
        threads,
        agg,
        decimals,
        overwrite,
        compact,
        geo,
        tempdir,
        semantics,
        transfer,
        out,
    ):
        if isinstance(resolution, str):
            raster_path = common.resolve_input_path(raster_input)
            resolution = common.resolve_resolution_mode(
                resolution,
                spec.name,
                raster_path,
                spec.min_res,
                spec.max_res,
            )
        parent_res: int = (
            int(parent_res)
            if parent_res is not None
            else (max(spec.min_res, int(resolution) - spec.default_parent_offset))
        )
        if out in ("list", "histogram", "interval"):
            ctx = click.get_current_context()
            if (
                ctx.get_parameter_source("agg")
                == click.core.ParameterSource.COMMANDLINE
            ):
                common.LOGGER.warning(
                    f"--out {out!r}: --agg has no effect (all contributing values are collected)"
                )
        return run_index(
            spec.name,
            raster_input,
            output_directory,
            resolution,
            parent_res,
            band,
            nodata_policy,
            emit_nodata_value,
            compression,
            threads,
            agg,
            decimals,
            overwrite,
            compact,
            geo,
            tempdir,
            semantics,
            transfer,
            out,
        )

    return cmd
