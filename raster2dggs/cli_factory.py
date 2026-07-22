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
            return int(value)
        except (ValueError, TypeError):
            self.fail(
                f"'{value}': expected a non-negative integer or 'none'", param, ctx
            )

    def get_metavar(self, param, ctx=None):
        return "INTEGER|none"


class BinEdgesParamType(click.ParamType):
    """Accepts 2+ strictly ascending, comma-separated bin edges (-inf/inf allowed)."""

    name = "EDGES"

    def convert(self, value, param, ctx):
        if isinstance(value, tuple):
            return value
        parts = [p.strip() for p in str(value).split(",")]
        try:
            edges = tuple(float(p) for p in parts)
        except ValueError:
            self.fail(f"'{value}': expected comma-separated numbers", param, ctx)
        if len(edges) < 2:
            self.fail("--hist-bins requires at least 2 edges", param, ctx)
        if any(b <= a for a, b in zip(edges, edges[1:])):
            self.fail(f"'{value}': edges must be strictly ascending", param, ctx)
        return edges

    def get_metavar(self, param, ctx=None):
        return "EDGE,EDGE[,...]"


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
    DGGS_Spec("rtea4r", "RTEA4R", const.MIN_RTEA4R, const.MAX_RTEA4R, 8),
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
    point: Optional[str],
    overlay: Optional[str],
    sample: Optional[str],
    valid_coverage_threshold: float = 0.0,
    hist_bins: Optional[tuple] = None,
    hist_width: Optional[float] = None,
    hist_origin: float = 0.0,
    hist_weight: str = "count",
    hist_normalize: str = "none",
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
        point,
        overlay,
        sample,
        valid_coverage_threshold,
        hist_bins,
        hist_width,
        hist_origin,
        hist_weight,
        hist_normalize,
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
        "-n",
        "--nodata",
        "nodata_policy",
        type=click.Choice(const.NODATA_POLICY_OPTIONS, case_sensitive=False),
        default=const.DEFAULTS["nodata_policy"],
        show_default=True,
        help=(
            "'omit' excludes nodata cells from output (default). "
            "'emit' includes them, writing the source raster nodata value (or --nodata-fill if set). "
            "Note: non-NaN emitted values participate in cell aggregation (see -a/--agg); "
            "if this is undesired, ensure your source nodata is NaN or override with --nodata-fill."
        ),
    )
    @click.option(
        "--nodata-fill",
        "emit_nodata_value",
        default=None,
        type=float,
        metavar="NUMBER",
        help=(
            "Override the value written for nodata cells when --nodata=emit. "
            "If omitted, the source raster nodata value is used (NaN if none is defined). "
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
        help="Number of threads to use when running in parallel. The default is determined dynamically as the total number of available cores, minus one.",
    )
    @click.option(
        "--point",
        is_flag=False,
        flag_value="value",
        default=None,
        type=click.Choice(
            [
                const.OutputSchema.VALUE.value,
                const.OutputSchema.LIST.value,
                const.OutputSchema.HISTOGRAM.value,
            ]
        ),
        metavar="OUTPUT",
        help=(
            "[Mutually exclusive with --overlay and --sample] "
            "Assign each pixel to the DGGS cell containing its centre (default). "
            "OUTPUT: 'value' (scalar per cell, default), "
            "'list' (sorted list of all contributing pixel values), "
            "'histogram' (value-count struct)."
        ),
    )
    @click.option(
        "--overlay",
        type=click.Choice([m.value for m in const.OverlayMode]),
        default=None,
        metavar="METHOD",
        help=(
            "[Mutually exclusive with --point and --sample] "
            "Area-based polygon intersection. METHOD: "
            "'weighted' (area-weighted mean), "
            "'mode' (majority class by overlap area), "
            "'mass-preserve' (area-weighted sum; conserves total — use when pixel value is a total count/mass), "
            "'density-preserve' (integrates density × pixel area; use when pixel value is a per-area rate), "
            "'fractions' (per-class area fractions → struct), "
            "'list' (all overlapping pixel values as a sorted list), "
            "'histogram' (value-count histogram of overlapping pixels)."
        ),
    )
    @click.option(
        "--sample",
        is_flag=False,
        flag_value="nn",
        default=None,
        type=click.Choice([i.value for i in const.Interp]),
        metavar="INTERP",
        help=(
            "[Mutually exclusive with --point and --overlay] "
            "Sample the raster at each DGGS cell centre. "
            "INTERP: 'nn' (nearest-neighbour, default), 'bilinear', 'bicubic', 'lanczos'."
        ),
    )
    @click.option(
        "-a",
        "--agg",
        default=const.DEFAULTS["aggfunc"],
        type=AggFuncListParamType(),
        show_default=True,
        help=(
            "Aggregation function(s) applied when multiple raster pixels map to the same DGGS cell "
            "(only relevant for --point). "
            f"Options: {', '.join(const.AGGFUNC_OPTIONS)}. "
            "Comma-separate multiple names (e.g. min,max) to produce a struct column per band."
        ),
    )
    @click.option(
        "-vct",
        "--valid-coverage-threshold",
        "valid_coverage_threshold",
        default=0.0,
        type=click.FloatRange(0.0, 1.0),
        show_default=True,
        help=(
            "Minimum fraction of each DGGS cell's overlapping raster area that must contain "
            "valid (non-nodata) pixels for the cell to receive a value. Applied per band. "
            "0.0 (default) keeps all cells with any valid data. "
            "Only meaningful for --overlay; ignored for --overlay mass-preserve "
            "(partial sums are correct values — filtering them would break mass conservation)."
        ),
    )
    @click.option(
        "--hist-bins",
        "hist_bins",
        default=None,
        type=BinEdgesParamType(),
        metavar="EDGE,EDGE[,...]",
        help=(
            "Explicit ascending histogram bin edges for --point/--overlay histogram. "
            "Bins are half-open [a, b); the last bin is closed. Values outside the "
            "range are dropped. Use -inf/inf for open-ended end bins. "
            "Mutually exclusive with --hist-width. Only meaningful with "
            "--point histogram or --overlay histogram."
        ),
    )
    @click.option(
        "--hist-width",
        "hist_width",
        default=None,
        type=float,
        metavar="FLOAT",
        help=(
            "Uniform histogram bin width for --point/--overlay histogram. Bins are "
            "[origin + k*width, origin + (k+1)*width); every finite value falls in a "
            "bin (nothing is dropped). Mutually exclusive with --hist-bins."
        ),
    )
    @click.option(
        "--hist-origin",
        "hist_origin",
        default=0.0,
        type=float,
        show_default=True,
        metavar="FLOAT",
        help="Anchor for --hist-width bins (a bin edge is placed at this value).",
    )
    @click.option(
        "--hist-weight",
        "hist_weight",
        default="count",
        type=click.Choice([w.value for w in const.HistWeight]),
        show_default=True,
        help=(
            "Histogram weighting: 'count' (number of contributing pixels), 'area' "
            "(each pixel weighted by its overlap area with the cell, in m^2; "
            "geodesic for geographic CRS). 'area' requires --overlay histogram."
        ),
    )
    @click.option(
        "--hist-normalize",
        "hist_normalize",
        default="none",
        type=click.Choice([n.value for n in const.HistNormalize]),
        show_default=True,
        help=(
            "Histogram normalization: 'none' (raw counts/weights), 'cell-area' "
            "(divide by the DGGS cell's area in m^2), 'valid-overlap' (divide by "
            "the total weight of all valid contributing pixels, so bins sum to <= 1). "
            "Non-integer results make counts float64."
        ),
    )
    @click.option(
        "-d",
        "--decimals",
        default=const.DEFAULTS["decimals"],
        type=DecimalsParamType(),
        help="Decimal places to round output values. 0 = integer; negative values round to tens (-1), hundreds (-2), etc. Use 'none' to disable rounding.",
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
        point,
        overlay,
        sample,
        valid_coverage_threshold,
        hist_bins,
        hist_width,
        hist_origin,
        hist_weight,
        hist_normalize,
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
        ctx = click.get_current_context()
        agg_from_cli = (
            ctx.get_parameter_source("agg") == click.core.ParameterSource.COMMANDLINE
        )
        if (
            point in ("list", "histogram") or overlay in ("list", "histogram")
        ) and agg_from_cli:
            effective = point or overlay
            common.LOGGER.warning(
                f"--point {effective}: --agg has no effect (all contributing values are collected)"
            )
        if sample is not None and agg_from_cli:
            common.LOGGER.warning(
                "--sample: --agg has no effect (one sample per DGGS cell)"
            )
        if emit_nodata_value is not None and nodata_policy == "omit":
            common.LOGGER.warning(
                "--nodata-fill has no effect when --nodata=omit (the default). "
                "Add --nodata emit to write the specified value for nodata cells."
            )
        hist_flag_names = {
            "hist_bins": "--hist-bins",
            "hist_width": "--hist-width",
            "hist_origin": "--hist-origin",
            "hist_weight": "--hist-weight",
            "hist_normalize": "--hist-normalize",
        }
        hist_from_cli = {
            p: ctx.get_parameter_source(p) == click.core.ParameterSource.COMMANDLINE
            for p in hist_flag_names
        }
        is_histogram_out = point == "histogram" or overlay == "histogram"
        if not is_histogram_out:
            for pname, flag in hist_flag_names.items():
                if hist_from_cli[pname]:
                    common.LOGGER.warning(
                        f"{flag}: no effect (output is not histogram)"
                    )
        elif hist_width is None and hist_from_cli["hist_origin"]:
            common.LOGGER.warning("--hist-origin has no effect without --hist-width")
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
            point,
            overlay,
            sample,
            valid_coverage_threshold,
            hist_bins,
            hist_width,
            hist_origin,
            hist_weight,
            hist_normalize,
        )

    return cmd
