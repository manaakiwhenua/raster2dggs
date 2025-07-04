from numbers import Number
import numpy as np
from pathlib import Path
import tempfile
from typing import Callable, Tuple, Union

import click
import click_log
import pandas as pd
import pyarrow as pa
from rasterio.enums import Resampling
import xarray as xr
import maidenhead as mh

import raster2dggs.constants as const
import raster2dggs.common as common
from raster2dggs import __version__

PAD_WIDTH = common.zero_padding("maidenhead")


def _maidenheadfunc(
    sdf: xr.DataArray,
    level: int,
    parent_level: int,
    nodata: Number = np.nan,
    band_labels: Tuple[str] = None,
) -> pa.Table:
    """
    Index a raster window to Maidenhead.
    Subsequent steps are necessary to resolve issues at the boundaries of windows.
    If windows are very small, or in strips rather than blocks, processing may be slower
    than necessary and the recommendation is to write different windows in the source raster.
    """
    sdf: pd.DataFrame = sdf.to_dataframe().drop(columns=["spatial_ref"]).reset_index()
    subset: pd.DataFrame = sdf.dropna()
    subset = subset[subset.value != nodata]
    subset = pd.pivot_table(
        subset, values=const.DEFAULT_NAME, index=["x", "y"], columns=["band"]
    ).reset_index()
    # Primary Maidenhead index
    maidenhead = [
        mh.to_maiden(lat, lon, level) for lat, lon in zip(subset["y"], subset["x"])
    ]  # Vectorised
    # Secondary (parent) Maidenhead index, used later for partitioning
    maidenhead_parent = [
        maidenhead_cell_to_parent(mh, parent_level) for mh in maidenhead
    ]
    subset = subset.drop(columns=["x", "y"])
    subset[f"maidenhead_{level:0{PAD_WIDTH}d}"] = pd.Series(
        maidenhead, index=subset.index
    )
    subset[f"maidenhead_{parent_level:0{PAD_WIDTH}d}"] = pd.Series(
        maidenhead_parent, index=subset.index
    )
    # Rename bands
    bands = sdf["band"].unique()
    band_names = dict(zip(bands, map(lambda i: band_labels[i - 1], bands)))
    for k, v in band_names.items():
        if band_names[k] is None:
            band_names[k] = str(bands[k - 1])
        else:
            band_names = band_names
    subset = subset.rename(columns=band_names)
    return pa.Table.from_pandas(subset)


def _maidenhead_parent_groupby(
    df, precision: int, aggfunc: Union[str, Callable], decimals: int
):
    """
    Function for aggregating the Maidenhead values per parent partition. Each partition will be run through with a
    pandas .groupby function. This step is to ensure there are no duplicate Maidenhead indices, which will certainly happen when indexing most raster datasets as Maidenhead has low precision.
    """
    if decimals > 0:
        return (
            df.groupby(f"maidenhead_{precision:0{PAD_WIDTH}d}")
            .agg(aggfunc)
            .round(decimals)
        )
    else:
        return (
            df.groupby(f"maidenhead_{precision:0{PAD_WIDTH}d}")
            .agg(aggfunc)
            .round(decimals)
            .astype("Int64")
        )


def maidenhead_cell_to_parent(cell: str, parent_level: int) -> str:
    """
    Returns cell parent at some offset level.
    """
    return cell[: parent_level * 2]


def maidenhead_cell_to_children_size(cell: str, desired_level: int) -> int:
    """
    Determine total number of children at some offset level.
    """
    level = len(cell) // 2
    if desired_level < level:
        return 0
    return 100 ** (desired_level - level)


def _maidenhead_compaction(
    df: pd.DataFrame, level: int, parent_level: int
) -> pd.DataFrame:
    """
    Returns a compacted version of the input dataframe.
    Compaction only occurs if all values (i.e. bands) of the input share common values across all sibling cells.
    Compaction will not be performed beyond parent_level or level.
    It assumes and requires that the input has unique DGGS cell values as the index.
    """
    unprocessed_indices = set(
        filter(lambda c: not pd.isna(c) and len(c) >= 2, set(df.index))
    )
    if not unprocessed_indices:
        return df
    compaction_map = {}
    for l in range(parent_level, level):
        parent_cells = list(
            map(lambda x: maidenhead_cell_to_parent(x, l), unprocessed_indices)
        )
        parent_groups = df.loc[list(unprocessed_indices)].groupby(list(parent_cells))
        for parent, group in parent_groups:
            if parent in compaction_map:
                continue
            expected_count = maidenhead_cell_to_children_size(parent, level)
            if len(group) == expected_count and all(group.nunique() == 1):
                compact_row = group.iloc[0]
                compact_row.name = parent  # Rename the index to the parent cell
                compaction_map[parent] = compact_row
                unprocessed_indices -= set(group.index)
    compacted_df = pd.DataFrame(list(compaction_map.values()))
    remaining_df = df.loc[list(unprocessed_indices)]
    result_df = pd.concat([compacted_df, remaining_df])
    result_df = result_df.rename_axis(df.index.name)
    return result_df


@click.command(context_settings={"show_default": True})
@click_log.simple_verbosity_option(common.LOGGER)
@click.argument("raster_input", type=click.Path(), nargs=1)
@click.argument("output_directory", type=click.Path(), nargs=1)
@click.option(
    "-r",
    "--resolution",
    required=True,
    type=click.Choice(
        list(map(str, range(const.MIN_MAIDENHEAD, const.MAX_MAIDENHEAD + 1)))
    ),
    help="Maidenhead level to index",
)
@click.option(
    "-pr",
    "--parent_res",
    required=False,
    type=click.Choice(
        list(map(str, range(const.MIN_MAIDENHEAD, const.MAX_MAIDENHEAD + 1)))
    ),
    help="Maidenhead 'parent' level to index and aggregate to. Defaults to level 1",
)
@click.option(
    "-u",
    "--upscale",
    default=const.DEFAULTS["upscale"],
    type=int,
    help="Upscaling factor, used to upsample input data on the fly; useful when the raster resolution is lower than the target Maidenhead precision. Default (1) applies no upscaling. The resampling method controls interpolation.",
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
    type=click.Choice(
        ["count", "mean", "sum", "prod", "std", "var", "min", "max", "median", "mode"]
    ),
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
    "--tempdir",
    default=const.DEFAULTS["tempdir"],
    type=click.Path(),
    help="Temporary data is created during the execution of this program. This parameter allows you to control where this data will be written.",
)
@click.version_option(version=__version__)
def maidenhead(
    raster_input: Union[str, Path],
    output_directory: Union[str, Path],
    resolution: str,
    parent_res: str,
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
    Ingest a raster image and index it using the Maidenhead Locator System (also known as QTH Locator and IARU Locator).

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
    )

    common.initial_index(
        "maidenhead",
        _maidenheadfunc,
        _maidenhead_parent_groupby,
        _maidenhead_compaction if compact else None,
        raster_input,
        output_directory,
        int(resolution),
        parent_res,
        warp_args,
        **kwargs,
    )
