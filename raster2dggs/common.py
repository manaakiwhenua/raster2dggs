import os
import errno
import tempfile
import logging
import numpy as np
import rioxarray
import dask
import click_log
import shutil

import rasterio as rio
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.dataset as ds
import json
import shapely
import pyproj

from typing import Union, Optional, Sequence, Callable
from pathlib import Path
from rasterio import crs
from rasterio.vrt import WarpedVRT
from rasterio.enums import Resampling
from tqdm import tqdm
from tqdm.dask import TqdmCallback
import dask.dataframe as dd
import xarray as xr

from concurrent.futures import ThreadPoolExecutor

from urllib.parse import urlparse
from rasterio.warp import calculate_default_transform

import raster2dggs.constants as const
import raster2dggs.indexerfactory as idxfactory

from raster2dggs.interfaces import IRasterIndexer

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
    geo: str,
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
        "geo": geo if geo != "none" else None,
    }

    return kwargs


def write_partition_as_geoparquet(
    pdf: pd.DataFrame,
    geom_func,
    base_dir: Union[str, Path],
    partition_col_name: str,
    compression: str,
) -> None:
    # Build shapely geometries for this partition
    geoms = pdf.index.map(geom_func)

    # Compute GeoParquet 1.1.0 extras
    valid = [g for g in geoms if (g is not None and not g.is_empty)]
    if len(valid):
        arr = np.asarray(shapely.bounds(geoms))  # Shapely 2.x vectorized
        m = ~np.isnan(arr).any(axis=1)
        bbox_vals = arr[m]
        bbox = [
            float(np.min(bbox_vals[:, 0])),
            float(np.min(bbox_vals[:, 1])),
            float(np.max(bbox_vals[:, 2])),
            float(np.max(bbox_vals[:, 3])),
        ]
        geometry_types = sorted({g.geom_type for g in valid})
    else:
        bbox = None
        geometry_types = []

    # Convert to WKB bytes (canonical encoding)
    pdf["geometry"] = shapely.to_wkb(geoms, hex=False)

    table = pa.Table.from_pandas(pdf, preserve_index=True)

    # Ensure geometry is Binary
    geom_idx = table.schema.get_field_index("geometry")
    if not pa.types.is_binary(table.field(geom_idx).type):
        geom_array = pa.array(table.column(geom_idx).to_pylist(), type=pa.binary())
        table = table.set_column(geom_idx, "geometry", geom_array)

    # GeoParquet 1.1.0 metadata
    crs_meta = pyproj.CRS.from_epsg(4326).to_json_dict()
    col_meta = {"encoding": "WKB", "crs": crs_meta}
    if geometry_types:
        col_meta["geometry_types"] = geometry_types
    if bbox is not None:
        col_meta["bbox"] = bbox

    geo_meta = {
        "version": "1.1.0",
        "primary_column": "geometry",
        "columns": {"geometry": col_meta},
    }
    existing_meta = table.schema.metadata or {}
    new_meta = {**existing_meta, b"geo": json.dumps(geo_meta).encode("utf-8")}
    table = table.replace_schema_metadata(new_meta)

    pq.write_to_dataset(
        table,
        root_path=str(base_dir),
        partition_cols=[partition_col_name],
        compression=compression,
        basename_template="part.{i}.parquet",
        existing_data_behavior="delete_matching",
        use_threads=True,
    )


def address_boundary_issues(
    indexer: IRasterIndexer,
    pq_input: tempfile.TemporaryDirectory,
    output: Path,
    resolution: int,
    parent_res: int,
    **kwargs,
) -> Path:
    """
    After "stage 1" processing, there is a DGGS cell and band value/s for
        each pixel in the input image. Partitions are hive-based, organised
        by parent cells at the given parent_res.

    Values are aggregated at the target resolution, to account for multiple
        pixels mapping to the same cell.

    This re-partitioning is necessary to address the issue of the same cell
        IDs being present in different windows of the original image
        windows.
    """
    if kwargs.get("overwrite", False) and Path(output).exists():
        shutil.rmtree(output)

    LOGGER.debug(f"Reading Stage 1 output ({pq_input})")
    index_col = indexer.index_col(resolution)
    partition_col = indexer.partition_col(parent_res)

    part_schema = pa.schema(
        [(partition_col, pa.string())]
    )  # Don't let this be inferred; e.g. geohash levels can be inferred variously as int or string
    # Keep hive partitions; coalese files per partition
    ddf = dd.read_parquet(
        pq_input,
        engine="pyarrow",
        aggregate_files=True,
        dataset={"partitioning": ds.partitioning(part_schema, flavor="hive")},
    )
    # Cols to aggregate (bands only)
    band_cols = [c for c in ddf.columns if not c.startswith(f"{indexer.dggs}_")]

    out_meta = pd.DataFrame(
        {
            partition_col: pd.Series([], dtype="string"),
            **{c: pd.Series([], dtype=ddf[c].dtype) for c in band_cols},
        }
    )
    out_meta.index = pd.Index([], name=index_col, dtype="object")
    with TqdmCallback(desc=f"Aggregating{'/compacting' if kwargs['compact'] else ''}"):
        ddf = ddf.map_partitions(
            indexer.parent_groupby,
            resolution,
            parent_res,
            kwargs["aggfunc"],
            kwargs["decimals"],
            meta=out_meta,
        )
        if kwargs["compact"]:
            ddf = ddf.map_partitions(
                indexer.compaction, resolution, parent_res, meta=out_meta
            )

        if kwargs["geo"]:

            # Create one delayed write task per Dask partition
            delayed_parts = ddf.to_delayed()

            geo_serialisation_method = (
                indexer.cell_to_polygon
                if kwargs["geo"] == "polygon"
                else indexer.cell_to_point
            )

            write_tasks = [
                dask.delayed(write_partition_as_geoparquet)(
                    part,
                    geo_serialisation_method,
                    output,
                    partition_col,
                    kwargs["compression"],
                )
                for part in delayed_parts
            ]

            # Execute writes with progress
            with TqdmCallback(desc="Writing GeoParquet"):
                dask.compute(*write_tasks)

        else:

            ddf.to_parquet(
                output,
                engine="pyarrow",
                partition_on=[partition_col],
                overwrite=kwargs["overwrite"],
                write_index=True,
                append=False,
                compression=kwargs["compression"],
            )

    LOGGER.debug("Stage 2 (aggregation) complete")

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
    Responsible for opening the raster_input, and performing DGGS indexing
        per window of a WarpedVRT.

    A WarpedVRT is used to enforce reprojection to https://epsg.io/4326,
        which is used for all DGGS indexing.

    It also allows on-the-fly resampling of the input, which is useful if
        the target DGGS resolution exceeds the resolution of the input.

    This function passes a path to a temporary directory (which contains
        the output of this "stage 1" processing) to a secondary function
        that addresses issues at the boundaries of raster windows.
    """
    indexer = idxfactory.indexer_instance(dggs)
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
                        "%d windows",
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

                        partition_col = indexer.partition_col(parent_res)
                        pq.write_to_dataset(
                            result,
                            root_path=tmpdir,
                            partition_cols=[partition_col],
                            basename_template=str(window.col_off)
                            + "."
                            + str(window.row_off)
                            + ".{i}.parquet",
                            use_threads=False,  # Already threading indexing and reading
                            existing_data_behavior="overwrite_or_ignore",  # Overwrite files with the same name; other existing files are ignored. Allows for an append workflow
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
