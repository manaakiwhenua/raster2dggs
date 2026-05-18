import gc
import os
import errno
import tempfile
import logging
import numpy as np
import rioxarray
import dask
import click
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

from typing import Any, Union, Optional, Sequence, Callable, List, Tuple
from pathlib import Path
from tqdm import tqdm
from tqdm.dask import TqdmCallback
import dask.dataframe as dd
import xarray as xr

from concurrent.futures import ThreadPoolExecutor

from urllib.parse import urlparse
from rasterio.warp import transform_bounds

import raster2dggs.constants as const
import raster2dggs.indexerfactory as idxfactory

from raster2dggs.interfaces import IRasterIndexer

LOGGER = logging.getLogger(__name__)
click_log.basic_config(LOGGER)


class ParentResolutionException(Exception):
    pass


def compute_pixel_area_m2(raster_input) -> tuple[float, float, float]:
    """
    Open the raster and return (pixel_area_m2, center_lat, center_lon).
    pixel_area_m2 is the mean pixel area: bounding-box geodesic area divided by pixel count.
    Bounds are projected to WGS84 for the area calculation.
    """
    with rio.open(raster_input, mode="r", sharing=False) as src:
        left, bottom, right, top = transform_bounds(src.crs, "EPSG:4326", *src.bounds)
        width, height = src.width, src.height

    bbox = shapely.geometry.box(left, bottom, right, top)
    area_m2, _ = pyproj.Geod(ellps="WGS84").geometry_area_perimeter(bbox)
    pixel_area_m2 = abs(area_m2) / (width * height)
    center_lat = (bottom + top) / 2
    center_lon = (left + right) / 2
    return pixel_area_m2, center_lat, center_lon


def resolve_resolution_mode(
    mode: str,
    dggs: str,
    raster_input,
    min_res: int,
    max_res: int,
) -> int:
    """
    Inspect the raster and return the integer resolution best matching the requested mode.

    Modes (all iterate from coarsest to finest resolution):
      smaller-than-pixel  — first resolution where cell area <= pixel area
      larger-than-pixel   — last resolution where cell area >= pixel area
      min-diff            — resolution where |cell area - pixel area| is minimised
    """
    import raster2dggs.indexerfactory as idxfactory

    indexer = idxfactory.indexer_instance(dggs)
    pixel_area_m2, center_lat, center_lon = compute_pixel_area_m2(raster_input)
    LOGGER.info(
        "Resolution mode '%s': pixel area=%.2f m², raster centre=(%.4f°N, %.4f°E)",
        mode,
        pixel_area_m2,
        center_lat,
        center_lon,
    )

    best_res = min_res
    min_area_diff = None

    for res in range(min_res, max_res + 1):
        cell_area = indexer.cell_area_m2(res, center_lat, center_lon)
        LOGGER.debug("  res %d: cell area=%.2f m²", res, cell_area)

        if mode == const.ResolutionMode.SMALLER_THAN_PIXEL:
            if cell_area <= pixel_area_m2:
                LOGGER.info("Auto-selected resolution %d (%s)", res, mode)
                return res

        elif mode == const.ResolutionMode.LARGER_THAN_PIXEL:
            if cell_area < pixel_area_m2:
                LOGGER.info("Auto-selected resolution %d (%s)", best_res, mode)
                return best_res
            best_res = res

        elif mode == const.ResolutionMode.MIN_DIFF:
            diff = abs(cell_area - pixel_area_m2)
            if min_area_diff is None or diff < min_area_diff:
                min_area_diff = diff
                best_res = res
            elif diff > min_area_diff:
                LOGGER.info("Auto-selected resolution %d (%s)", best_res, mode)
                return best_res

    LOGGER.info("Auto-selected resolution %d (%s, end of range)", best_res, mode)
    return best_res


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


def create_aggfuncs(
    names: Tuple[str, ...],
    decimals: Optional[int] = None,
) -> List[Tuple[str, Union[str, Callable]]]:
    """Convert a tuple of aggfunc name strings to (name, callable_or_str) pairs."""

    def _mode(x: pd.Series) -> Any:
        binned = x.round(decimals) if decimals is not None else x
        m = pd.Series.mode(binned, dropna=False)
        return m.iloc[0] if not m.empty else np.nan

    def _majority(x: pd.Series) -> Any:
        """Most common value if it appears in >50% of all contributing pixels, else NaN."""
        valid = x.dropna()
        if valid.empty:
            return np.nan
        binned = valid.round(decimals) if decimals is not None else valid
        counts = binned.value_counts()
        if counts.iloc[0] / len(x) > 0.5:
            return counts.index[0]
        return np.nan

    result = []
    for name in names:
        if name == "mode":
            logging.warning(
                "Mode aggregation: arbitrary behaviour: if there is more than one mode when aggregating, only the first value will be recorded."
            )
            result.append((name, _mode))
        elif name == "majority":
            result.append((name, _majority))
        elif name == "range":
            result.append((name, lambda x: x.max() - x.min()))
        else:
            result.append((name, name))  # pandas knows these strings
    return result


def assemble_kwargs(
    compression: str,
    threads: int,
    aggfuncs: List[Tuple[str, Union[str, Callable]]],
    decimals: int,
    overwrite: bool,
    compact: bool,
    geo: str,
    semantics: str = const.Semantics.POINT_CENTER_STRICT,
    transfer: str = const.Transfer.ASSIGN_CENTERS,
    out: str = const.OutputSchema.VALUE,
) -> dict:
    return {
        "compression": compression,
        "threads": threads,
        "aggfuncs": aggfuncs,
        "decimals": decimals,
        "overwrite": overwrite,
        "compact": compact,
        "geo": geo if geo != "none" else None,
        "semantics": semantics,
        "transfer": transfer,
        "out": out,
    }


def write_partition_as_geoparquet(
    pdf: pd.DataFrame,
    geom_func,
    base_dir: Union[str, Path],
    partition_col_name: str,
    compression: str,
    schema: pa.Schema,
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

    table = pa.Table.from_pandas(
        pdf,
        schema=schema.append(pa.field("geometry", pa.binary())),
        preserve_index=True,
    )

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
    # Capture source dtypes before map_partitions changes them
    source_dtypes = {c: ddf[c].dtype for c in band_cols}

    out = kwargs.get("out", "value")

    decimals = kwargs.get("decimals")

    aggfuncs = kwargs.get("aggfuncs", [("mean", "mean")])

    if out in ("list", "histogram") or (out == "value" and len(aggfuncs) > 1):
        out_meta = pd.DataFrame(
            {
                partition_col: pd.Series([], dtype="string"),
                **{c: pd.Series([], dtype="object") for c in band_cols},
            }
        )
        tqdm_label = (
            "Collecting"
            if out != "value"
            else f"Aggregating{'/compacting' if kwargs['compact'] else ''}"
        )
    else:
        out_meta = pd.DataFrame(
            {
                partition_col: pd.Series([], dtype="string"),
                **{
                    c: pd.Series(
                        [],
                        dtype=(
                            # decimals=0 means integer output
                            "Int64"
                            if decimals == 0
                            # float32 is promoted to float64 before rounding so
                            # that decimal values are exactly representable
                            else (
                                "float64"
                                if decimals is not None
                                and source_dtypes[c] == np.float32
                                else source_dtypes[c]
                            )
                        ),
                    )
                    for c in band_cols
                },
            }
        )
        tqdm_label = f"Aggregating{'/compacting' if kwargs['compact'] else ''}"

    out_meta.index = pd.Index([], name=index_col, dtype="string")

    with TqdmCallback(desc=tqdm_label):
        if out == "list":
            mp_func = indexer.parent_groupby_list
            mp_args = (resolution, parent_res, decimals)
        elif out == "histogram":
            mp_func = indexer.parent_groupby_histogram
            mp_args = (resolution, parent_res, decimals)
        else:
            mp_func = indexer.parent_groupby
            mp_args = (resolution, parent_res, aggfuncs, decimals)

        ddf = ddf.map_partitions(mp_func, *mp_args, meta=out_meta)

        if kwargs["compact"]:
            ddf = ddf.map_partitions(
                indexer.compaction, resolution, parent_res, meta=out_meta
            )

        def _element_type(src_dtype):
            if decimals == 0:
                return pa.int64()
            if decimals is not None and src_dtype == np.float32:
                return pa.float64()
            return pa.from_numpy_dtype(src_dtype)

        common_fields = [
            pa.field(index_col, pa.string()),
            pa.field(partition_col, pa.string()),
        ]
        if out == "list":
            write_schema = pa.schema(
                common_fields
                + [
                    pa.field(c, pa.list_(_element_type(source_dtypes[c])))
                    for c in band_cols
                ]
            )
        elif out == "histogram":
            write_schema = pa.schema(
                common_fields
                + [
                    pa.field(
                        c,
                        pa.struct(
                            [
                                pa.field(
                                    "values",
                                    pa.list_(_element_type(source_dtypes[c])),
                                ),
                                pa.field("counts", pa.list_(pa.int64())),
                            ]
                        ),
                    )
                    for c in band_cols
                ]
            )
        elif len(aggfuncs) > 1:
            write_schema = pa.schema(
                common_fields
                + [
                    pa.field(
                        c,
                        pa.struct(
                            [
                                pa.field(agg_name, _element_type(source_dtypes[c]))
                                for agg_name, _ in aggfuncs
                            ]
                        ),
                    )
                    for c in band_cols
                ]
            )
        else:
            write_schema = pa.Schema.from_pandas(out_meta, preserve_index=True)

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
                    write_schema,
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
                schema=write_schema,
            )

    LOGGER.debug("Stage 2 (aggregation) complete")

    return output


def validate_transfer_config(semantics: str, transfer: str, out: str) -> None:
    if (semantics, transfer) in const.INAPPROPRIATE:
        raise click.UsageError(
            f"--transfer {transfer!r} is inappropriate for --semantics {semantics!r}. "
            f"See the semantics × transfer matrix in the documentation."
        )
    if (semantics, transfer, out) not in const.IMPLEMENTED:
        raise NotImplementedError(
            f"--semantics {semantics!r} / --transfer {transfer!r} / --out {out!r} "
            f"is a valid combination but is not yet implemented."
        )


def initial_index(
    dggs: str,
    raster_input: Union[Path, str],
    output: Path,
    resolution: int,
    parent_res: Union[None, int],
    bands: Optional[Sequence[Union[int, str]]] = None,
    nodata_policy: str = "omit",
    emit_nodata_value: Optional[Union[int, float]] = None,
    **kwargs,
) -> Path:
    """
    Responsible for opening the raster_input and performing DGGS indexing per window.

    Pixel centre coordinates are projected from the source CRS to WGS84 using
    pyproj.Transformer, preserving original raster values without resampling.

    This function passes a path to a temporary directory (which contains
        the output of this "stage 1" processing) to a secondary function
        that addresses issues at the boundaries of raster windows.
    """
    validate_transfer_config(kwargs["semantics"], kwargs["transfer"], kwargs["out"])

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

        with rio.Env():
            with rio.open(raster_input, mode="r", sharing=False) as src:
                LOGGER.debug("Source CRS: %s", src.crs)
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

                transformer = pyproj.Transformer.from_crs(
                    src.crs, "EPSG:4326", always_xy=True
                )
                LOGGER.debug("Coordinate transformer: %s → EPSG:4326", src.crs)

                da: xr.Dataset = rioxarray.open_rasterio(
                    src,
                    lock=dask.utils.SerializableLock(),
                    masked=False,
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

                windows = [window for _, window in src.block_windows()]
                LOGGER.debug(
                    "%d windows",
                    len(windows),
                )

                selected_labels = tuple([labels_by_index[i] for i in selected_indices])
                compression = kwargs["compression"]
                nodata = src.nodata

                def process(window):
                    sdf = da.rio.isel_window(window)

                    result = indexer.index_func(
                        sdf,
                        resolution,
                        parent_res,
                        nodata,
                        band_labels=selected_labels,
                        nodata_policy=nodata_policy,
                        emit_nodata_value=emit_nodata_value,
                        transformer=transformer,
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

                try:
                    with ThreadPoolExecutor(
                        max_workers=kwargs["threads"]
                    ) as executor, tqdm(
                        total=len(windows), desc="Raster windows"
                    ) as pbar:
                        for _ in executor.map(process, windows, chunksize=1):
                            pbar.update(1)
                finally:
                    da.close()
                    # da, transformer, and process are closure cells and won't
                    # be freed by reference counting alone if they're in a dask
                    # task-graph cycle.  Explicitly delete all three and collect
                    # now, while still inside rio.open(), so the GDAL/PROJ
                    # objects they hold are torn down during normal execution
                    # rather than at interpreter shutdown (which causes a
                    # silent "Error in sys.excepthook" crash for non-WGS84
                    # rasters).
                    del da, transformer, process
                    gc.collect()
            LOGGER.debug("Stage 1 (primary indexing) complete")
            return address_boundary_issues(
                indexer,
                tmpdir,
                output,
                resolution,
                parent_res,
                **kwargs,
            )
