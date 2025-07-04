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
from s2sphere import LatLng, CellId
import xarray as xr

import raster2dggs.constants as const
import raster2dggs.common as common
from raster2dggs import __version__

PAD_WIDTH = common.zero_padding("s2")


def _s2func(
    sdf: xr.DataArray,
    resolution: int,
    parent_res: int,
    nodata: Number = np.nan,
    band_labels: Tuple[str] = None,
) -> pa.Table:
    """
    Index a raster window to S2.
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
    # S2 index
    cells = [
        CellId.from_lat_lng(LatLng.from_degrees(lat, lon))
        for lat, lon in zip(subset["y"], subset["x"])
    ]
    s2 = [cell.parent(resolution).to_token() for cell in cells]
    s2_parent = [cell.parent(parent_res).to_token() for cell in cells]
    subset = subset.drop(columns=["x", "y"])
    subset[f"s2_{resolution:0{PAD_WIDTH}d}"] = pd.Series(s2, index=subset.index)
    subset[f"s2_{parent_res:0{PAD_WIDTH}d}"] = pd.Series(s2_parent, index=subset.index)
    # Renaming columns to actual band labels
    bands = sdf["band"].unique()
    band_names = dict(zip(bands, map(lambda i: band_labels[i - 1], bands)))
    for k, v in band_names.items():
        if band_names[k] is None:
            band_names[k] = str(bands[k - 1])
        else:
            band_names = band_names
    subset = subset.rename(columns=band_names)
    return pa.Table.from_pandas(subset)


def _s2_parent_groupby(
    df, resolution: int, aggfunc: Union[str, Callable], decimals: int
):
    """
    Function for aggregating the S2 resolution values per parent partition. Each partition will be run through with a
    pandas .groupby function. This step is to ensure there are no duplicate S2 values, which will happen when indexing a
    high resolution raster at a coarser S2 resolution.
    """
    if decimals > 0:
        return df.groupby(f"s2_{resolution:0{PAD_WIDTH}d}").agg(aggfunc).round(decimals)
    else:
        return (
            df.groupby(f"s2_{resolution:0{PAD_WIDTH}d}")
            .agg(aggfunc)
            .round(decimals)
            .astype("Int64")
        )


def s2_cell_to_children_size(cell: CellId, desired_resolution: int) -> int:
    """
    Determine total number of children at some offset resolution
    """
    # return sum(1 for _ in cell.children(desired_resolution)) # Expensive eumeration
    cell_level = cell.level()
    if cell_level == 0:
        # At level 0, there are 6 initial cells on the S2 sphere.
        # Each of these divides into 4^n children at any subsequent level n.
        return 6 * (4 ** (desired_resolution - 1))
    # For levels greater than 0, the cell divides into 4^(n-m) children
    return 4 ** (desired_resolution - cell_level)


def _s2_compaction(df: pd.DataFrame, resolution: int, parent_res: int) -> pd.DataFrame:
    """
    Returns a compacted version of the input dataframe.
    Compaction only occurs if all values (i.e. bands) of the input share common values across all sibling cells.
    Compaction will not be performed beyond parent_res or resolution.
    It assumes and requires that the input has unique DGGS cell values as the index.
    """
    unprocessed_indices = set(
        map(
            lambda c: c.to_token(),
            filter(
                lambda c: c.is_valid(),
                map(
                    lambda c: CellId.from_token(c),
                    filter(lambda c: not pd.isna(c), set(df.index)),
                ),
            ),
        )
    )
    if not unprocessed_indices:
        return df
    compaction_map = {}
    for r in range(parent_res, resolution):
        parent_cells = map(
            lambda token: CellId.from_token(token).parent(r).to_token(),
            unprocessed_indices,
        )
        parent_groups = df.loc[list(unprocessed_indices)].groupby(list(parent_cells))
        for parent, group in parent_groups:
            if parent in compaction_map:
                continue
            expected_count = s2_cell_to_children_size(
                CellId.from_token(parent), resolution
            )
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
    type=click.Choice(list(map(str, range(const.MIN_S2, const.MAX_S2 + 1)))),
    help="S2 resolution to index",
)
@click.option(
    "-pr",
    "--parent_res",
    required=False,
    type=click.Choice(list(map(str, range(const.MIN_S2, const.MAX_S2 + 1)))),
    help="S2 parent resolution to index and aggregate to. Defaults to resolution - 6",
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
def s2(
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
    Ingest a raster image and index it to the S2 DGGS.

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
        "s2",
        _s2func,
        _s2_parent_groupby,
        _s2_compaction if compact else None,
        raster_input,
        output_directory,
        int(resolution),
        parent_res,
        warp_args,
        **kwargs,
    )
