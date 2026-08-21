import atexit
import dataclasses
import errno
import gc
import json
import logging
import multiprocessing
import os
import shutil
import sys
import tempfile
from collections.abc import Callable, Sequence
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import click
import click_log
import dask
import dask.dataframe as dd
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import pyproj
import rasterio as rio
import rioxarray
import shapely
from rasterio.warp import transform_bounds
from tqdm import tqdm
from tqdm.dask import TqdmCallback

import raster2dggs.constants as const
import raster2dggs.histogram as histogram
import raster2dggs.indexerfactory as idxfactory
from raster2dggs.interfaces import IRasterIndexer
from raster2dggs.profiling import PROFILER
from raster2dggs.transfers.assign_centers import _AssignCentersIndexer
from raster2dggs.transfers.interpolation import _SampleIndexer
from raster2dggs.transfers.overlay import _OverlayIndexer

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
            f"Parent resolution ({parent_res}) must be less than target resolution ({resolution})"
        )


def resolve_input_path(raster_input: str | Path) -> str | Path:
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
    names: tuple[str, ...],
    decimals: int | None = None,
) -> list[tuple[str, str | Callable]]:
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
            LOGGER.warning(
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


def resolve_to_internal(
    point: str | None,
    overlay: str | None,
    sample: str | None,
) -> dict:
    """Map CLI flags to the internal (transfer_key, op, out_key) triple."""
    if overlay is not None:
        return {
            const.OverlayMode.WEIGHTED: {
                "transfer": const.Transfer.OVERLAY_WEIGHTED,
                "op": const.Op.MEAN,
                "out": const.OutputSchema.VALUE,
            },
            const.OverlayMode.MODE: {
                "transfer": const.Transfer.OVERLAY_MODE,
                "op": const.Op.MAJORITY,
                "out": const.OutputSchema.VALUE,
            },
            const.OverlayMode.MASS_PRESERVE: {
                "transfer": const.Transfer.MASS_PRESERVE,
                "op": const.Op.SUM,
                "out": const.OutputSchema.VALUE,
            },
            const.OverlayMode.DENSITY_PRESERVE: {
                "transfer": const.Transfer.OVERLAY_WEIGHTED,
                "op": const.Op.WSUM,
                "out": const.OutputSchema.VALUE,
            },
            const.OverlayMode.FRACTIONS: {
                "transfer": const.Transfer.OVERLAY_WEIGHTED,
                "op": const.Op.FRAC,
                "out": const.OutputSchema.FRACTIONS,
            },
            const.OverlayMode.LIST: {
                "transfer": const.Transfer.OVERLAY_COLLECT,
                "op": const.Op.VALUES,
                "out": const.OutputSchema.LIST,
            },
            const.OverlayMode.HISTOGRAM: {
                "transfer": const.Transfer.OVERLAY_COLLECT,
                "op": const.Op.VALUES,
                "out": const.OutputSchema.HISTOGRAM,
            },
        }[overlay]
    if sample is not None:
        return {
            "transfer": const.Transfer.SAMPLE,
            "op": None,
            "out": const.OutputSchema.VALUE,
            "interp": sample,
        }
    # point (default)
    out = const.OutputSchema(point) if point is not None else const.OutputSchema.VALUE
    return {"transfer": const.Transfer.ASSIGN_CENTERS, "op": None, "out": out}


def _build_histogram_spec(kwargs: dict) -> histogram.HistogramSpec | None:
    """Build a HistogramSpec from raw --hist-* CLI values, or None when the
    resolved output isn't histogram (the --hist-* flags then have no effect)."""
    if kwargs.get("out") != const.OutputSchema.HISTOGRAM:
        return None
    hist_bins = kwargs.get("hist_bins")
    return histogram.HistogramSpec(
        edges=tuple(hist_bins) if hist_bins else None,
        width=kwargs.get("hist_width"),
        origin=kwargs.get("hist_origin") or 0.0,
        weight=kwargs.get("hist_weight") or const.HistWeight.COUNT,
        normalize=kwargs.get("hist_normalize") or const.HistNormalize.NONE,
    )


def validate_config(
    point: str | None,
    overlay: str | None,
    sample: str | None,
    hist_bins: Sequence[float] | None = None,
    hist_width: float | None = None,
    hist_weight: str | None = None,
    hist_normalize: str | None = None,
) -> None:
    if overlay is not None and sample is not None:
        raise click.UsageError("--overlay and --sample are mutually exclusive")
    if point is not None and overlay is not None:
        raise click.UsageError("--point and --overlay are mutually exclusive")
    if point is not None and sample is not None:
        raise click.UsageError("--point and --sample are mutually exclusive")
    if hist_bins is not None and hist_width is not None:
        raise click.UsageError("--hist-bins and --hist-width are mutually exclusive")
    if hist_weight == const.HistWeight.AREA and overlay != const.OverlayMode.HISTOGRAM:
        raise click.UsageError(
            "--hist-weight area requires --overlay histogram "
            "(area weighting is undefined for --point/--sample)"
        )
    if (
        hist_weight == const.HistWeight.COUNT
        and hist_normalize == const.HistNormalize.CELL_AREA
    ):
        raise click.UsageError(
            "--hist-weight count with --hist-normalize cell-area is not supported "
            "(a pixel count divided by area is a density, not a count or a fraction -- "
            "use --hist-weight area instead)"
        )


def assemble_kwargs(
    compression: str,
    processes: int,
    aggfuncs: list[tuple[str, str | Callable]],
    decimals: int,
    overwrite: bool,
    compact: bool,
    geo: str,
    point: str | None = None,
    overlay: str | None = None,
    sample: str | None = None,
    valid_coverage_threshold: float = 0.0,
    hist_bins: tuple[float, ...] | None = None,
    hist_width: float | None = None,
    hist_origin: float = 0.0,
    hist_weight: str = "count",
    hist_normalize: str = "none",
    cell_id: str = "string",
) -> dict:
    return {
        "compression": compression,
        "processes": processes,
        "aggfuncs": aggfuncs,
        "decimals": decimals,
        "overwrite": overwrite,
        "compact": compact,
        "geo": geo if geo != "none" else None,
        "point": point,
        "overlay": overlay,
        "sample": sample,
        "valid_coverage_threshold": valid_coverage_threshold,
        "hist_bins": hist_bins,
        "hist_width": hist_width,
        "hist_origin": hist_origin,
        "hist_weight": hist_weight,
        "hist_normalize": hist_normalize,
        "cell_id": cell_id,
    }


def write_partition_as_geoparquet(
    pdf: pd.DataFrame,
    geom_func,
    base_dir: str | Path,
    partition_col_name: str,
    compression: str,
    schema: pa.Schema,
    cells_to_string=None,
) -> None:
    # Build shapely geometries for this partition. geom_func takes the working
    # cell form, so geometry comes first and any string conversion after.
    geoms = pdf.index.map(geom_func)
    if cells_to_string is not None:
        pdf = pdf.copy(deep=False)
        pdf.index = pd.Index(
            cells_to_string(pdf.index), name=pdf.index.name, dtype="string"
        )
        pdf[partition_col_name] = pd.Series(
            cells_to_string(pdf[partition_col_name]), index=pdf.index, dtype="string"
        )

    # Compute GeoParquet 1.1.0 extras
    valid = [g for g in geoms if (g is not None and not g.is_empty)]
    if len(valid):
        arr = np.asarray(shapely.bounds(geoms))  # Shapely 2.x vectorised
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


def _element_type(src_dtype, decimals) -> pa.DataType:
    if decimals is not None and decimals <= 0:
        return pa.int64()
    if decimals is not None:  # decimals > 0: always float64
        return pa.float64()
    return pa.from_numpy_dtype(src_dtype)


def _build_output_meta(
    *,
    transfer: str,
    out: str,
    aggfuncs: list,
    decimals,
    source_dtypes: dict,
    band_cols: list,
    partition_col: str,
    index_col: str,
    compact: bool,
    interp: str,
    cell_dtype: str = "string",
) -> tuple[pd.DataFrame, str]:
    if (
        transfer == const.Transfer.SAMPLE
        or (
            transfer in const.OVERLAY_TRANSFER_KEYS
            and out
            not in (
                const.OutputSchema.FRACTIONS,
                const.OutputSchema.LIST,
                const.OutputSchema.HISTOGRAM,
            )
        )
        or (out == const.OutputSchema.VALUE and len(aggfuncs) == 1)
    ):
        out_meta = pd.DataFrame(
            {
                partition_col: pd.Series([], dtype=cell_dtype),
                **{
                    c: pd.Series(
                        [],
                        dtype=(
                            "Int64"
                            if decimals is not None and decimals <= 0
                            # decimals > 0: always float64 regardless of source
                            # dtype — aggregations on integer rasters (e.g. mean)
                            # produce floats, and rounding to dp implies a float result
                            else "float64" if decimals is not None else source_dtypes[c]
                        ),
                    )
                    for c in band_cols
                },
            }
        )
        _compacting = "/compacting" if compact else ""
        if transfer == const.Transfer.SAMPLE:
            tqdm_label = f"Sampling ({interp}){_compacting}"
        elif transfer in const.OVERLAY_TRANSFER_KEYS:
            tqdm_label = f"Overlay{_compacting}"
        else:
            tqdm_label = f"Aggregating{_compacting}"
    else:
        # list, histogram, or multi-agg value — object-typed columns
        out_meta = pd.DataFrame(
            {
                partition_col: pd.Series([], dtype=cell_dtype),
                **{c: pd.Series([], dtype="object") for c in band_cols},
            }
        )
        tqdm_label = (
            "Collecting"
            if out != const.OutputSchema.VALUE
            else f"Aggregating{'/compacting' if compact else ''}"
        )
    out_meta.index = pd.Index([], name=index_col, dtype=cell_dtype)
    return out_meta, tqdm_label


def _build_write_schema(
    *,
    out: str,
    aggfuncs: list,
    band_cols: list,
    decimals,
    source_dtypes: dict,
    index_col: str,
    partition_col: str,
    out_meta: pd.DataFrame,
    hist_spec: histogram.HistogramSpec | None = None,
    cell_type: pa.DataType | None = None,
) -> pa.Schema:
    cell_type = cell_type if cell_type is not None else pa.string()
    common_fields = [
        pa.field(index_col, cell_type),
        pa.field(partition_col, cell_type),
    ]
    if out == const.OutputSchema.FRACTIONS:
        frac_struct = pa.struct(
            [
                pa.field("classes", pa.list_(pa.int64())),
                pa.field("fractions", pa.list_(pa.float64())),
            ]
        )
        return pa.schema(common_fields + [pa.field(c, frac_struct) for c in band_cols])
    if out == const.OutputSchema.LIST:
        return pa.schema(
            common_fields
            + [
                pa.field(c, pa.list_(_element_type(source_dtypes[c], decimals)))
                for c in band_cols
            ]
        )
    if out == const.OutputSchema.HISTOGRAM:
        fields = [
            pa.field(
                c,
                histogram.histogram_struct_type(hist_spec, decimals, source_dtypes[c]),
            )
            for c in band_cols
        ]
        return pa.schema(common_fields + fields)
    if len(aggfuncs) > 1:
        return pa.schema(
            common_fields
            + [
                pa.field(
                    c,
                    pa.struct(
                        [
                            pa.field(
                                agg_name, _element_type(source_dtypes[c], decimals)
                            )
                            for agg_name, _ in aggfuncs
                        ]
                    ),
                )
                for c in band_cols
            ]
        )
    # The meta carries the working cell form; the written form follows
    # cell_type. Coercing a copy of the meta and letting from_pandas derive the
    # fields keeps the schema identical to one built from text cells directly.
    schema_meta = out_meta
    if cell_type == pa.string() and str(out_meta.index.dtype) != "string":
        schema_meta = out_meta.copy()
        schema_meta.index = out_meta.index.astype("string")
        schema_meta[partition_col] = schema_meta[partition_col].astype("string")
    return pa.Schema.from_pandas(schema_meta, preserve_index=True)


def _write_output(
    ddf,
    *,
    output: Path,
    geo,
    compression: str,
    partition_col: str,
    overwrite: bool,
    write_schema: pa.Schema,
    indexer: IRasterIndexer,
    cells_to_string=None,
) -> None:
    if geo:
        delayed_parts = ddf.to_delayed()
        geo_serialisation_method = (
            indexer.cell_to_polygon if geo == "polygon" else indexer.cell_to_point
        )
        write_tasks = [
            dask.delayed(write_partition_as_geoparquet)(
                part,
                geo_serialisation_method,
                output,
                partition_col,
                compression,
                write_schema,
                cells_to_string,
            )
            for part in delayed_parts
        ]
        with TqdmCallback(desc="Writing GeoParquet"):
            dask.compute(*write_tasks)
    else:
        ddf.to_parquet(
            output,
            engine="pyarrow",
            partition_on=[partition_col],
            overwrite=overwrite,
            write_index=True,
            append=False,
            compression=compression,
            schema=write_schema,
        )


def _stage1_partitioning(partition_col: str, cell_type: pa.DataType) -> ds.Partitioning:
    """Hive partitioning for the Stage 1 store. The partition column type is
    declared rather than inferred: string cell IDs otherwise read as integers
    for some backends (geohash levels among them)."""
    return ds.partitioning(pa.schema([(partition_col, cell_type)]), flavor="hive")


def _read_stage1_parent(
    pq_input: str, partition_col: str, cell_type: pa.DataType, parent
) -> pd.DataFrame:
    """Read every Stage 1 row belonging to one parent cell."""
    dataset = ds.dataset(
        pq_input,
        format="parquet",
        partitioning=_stage1_partitioning(partition_col, cell_type),
    )
    # A typed scalar: a bare Python int past 2^63 (S2 faces 4-5) overflows the
    # default int64 conversion.
    return dataset.to_table(
        filter=ds.field(partition_col) == pa.scalar(parent, type=cell_type)
    ).to_pandas()


def _read_stage1_by_parent(
    pq_input, partition_col: str, cell_type: pa.DataType
) -> dd.DataFrame:
    """Read the Stage 1 store as one partition per parent cell.

    The aggregation that follows groups rows by cell, and a cell spanning two
    raster windows has its pixels in several Stage 1 files, so every row for a
    cell must reach the same partition. Parent directories are the coarsest
    grouping that guarantees it, and hold a bounded number of cells (see
    ``DGGS_Spec.default_parent_offset``).
    """
    dataset = ds.dataset(
        str(pq_input),
        format="parquet",
        partitioning=_stage1_partitioning(partition_col, cell_type),
    )
    parents = sorted(
        set(
            pa.compute.unique(
                dataset.to_table(columns=[partition_col])[partition_col]
            ).to_pylist()
        )
    )
    LOGGER.debug("Stage 1 store spans %d parent cells", len(parents))
    meta = _read_stage1_parent(
        str(pq_input), partition_col, cell_type, parents[0]
    ).head(0)
    # --overlay list/histogram/fractions hold dicts and lists in their band
    # columns, which dask's string conversion would stringify. Dtypes are fixed
    # when the graph is built, so scoping it to construction is enough.
    with dask.config.set({"dataframe.convert-string": False}):
        return dd.from_map(
            _read_stage1_parent,
            [str(pq_input)] * len(parents),
            [partition_col] * len(parents),
            [cell_type] * len(parents),
            parents,
            meta=meta,
        )


def _cells_frame_to_string(
    pdf: pd.DataFrame, indexer: IRasterIndexer, partition_col: str
) -> pd.DataFrame:
    """Convert a partition's working-form cell index and parent column to strings."""
    pdf = pdf.copy(deep=False)
    pdf.index = pd.Index(
        indexer.cells_to_string(pdf.index), name=pdf.index.name, dtype="string"
    )
    pdf[partition_col] = pd.Series(
        indexer.cells_to_string(pdf[partition_col]), index=pdf.index, dtype="string"
    )
    return pdf


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

    ddf = _read_stage1_by_parent(pq_input, partition_col, indexer.CELL_ARROW_TYPE)
    band_cols = [c for c in ddf.columns if not c.startswith(f"{indexer.dggs}_")]
    # Capture source dtypes before map_partitions changes them.
    # Stage 1 output for --overlay list/histogram already holds aggregated
    # python list/dict objects, so re-reading it here gives dtype "object" --
    # not the original raster pixel dtype. Prefer the dtypes captured directly
    # from the source raster (kwargs["source_pixel_dtypes"], set in
    # initial_index) when available; they agree with ddf[c].dtype for every
    # other output mode, where Stage 1 still holds unaggregated scalar values.
    source_pixel_dtypes = kwargs.get("source_pixel_dtypes") or {}
    source_dtypes = {c: source_pixel_dtypes.get(c, ddf[c].dtype) for c in band_cols}

    out = kwargs.get("out", const.OutputSchema.VALUE)
    transfer = kwargs.get("transfer", const.Transfer.ASSIGN_CENTERS)
    decimals = kwargs.get("decimals")
    aggfuncs = kwargs.get("aggfuncs", [("mean", "mean")])
    # String output needs a conversion for backends that carry integers
    # internally; for string backends it is already the working form.
    emit_string = (
        kwargs.get("cell_id", const.CellId.STRING) == const.CellId.STRING
        and indexer.CELL_ARROW_TYPE != pa.string()
    )
    output_cell_type = (
        pa.string()
        if kwargs.get("cell_id", const.CellId.STRING) == const.CellId.STRING
        else indexer.CELL_ARROW_TYPE
    )

    out_meta, tqdm_label = _build_output_meta(
        transfer=transfer,
        out=out,
        aggfuncs=aggfuncs,
        decimals=decimals,
        source_dtypes=source_dtypes,
        band_cols=band_cols,
        partition_col=partition_col,
        index_col=index_col,
        compact=kwargs["compact"],
        interp=kwargs.get("interp", const.Interp.NN),
        cell_dtype=indexer.cell_pd_dtype,
    )

    with PROFILER.phase("stage2.total"), TqdmCallback(desc=tqdm_label):
        if transfer == const.Transfer.SAMPLE or transfer in const.OVERLAY_TRANSFER_KEYS:
            mp_func = indexer.parent_groupby_nn
            mp_args = (resolution, parent_res, decimals)
        elif out == const.OutputSchema.LIST:
            mp_func = indexer.parent_groupby_list
            mp_args = (resolution, parent_res, decimals)
        elif out == const.OutputSchema.HISTOGRAM:
            mp_func = indexer.parent_groupby_histogram
            mp_args = (resolution, parent_res, decimals, kwargs.get("hist_spec"))
        else:
            mp_func = indexer.parent_groupby
            mp_args = (resolution, parent_res, aggfuncs, decimals)

        # The partition count caps what Stage 2 can parallelise.
        PROFILER.note("stage2_partitions", ddf.npartitions)

        ddf = ddf.map_partitions(mp_func, *mp_args, meta=out_meta)

        if kwargs["compact"]:
            ddf = ddf.map_partitions(
                indexer.compaction, resolution, parent_res, meta=out_meta
            )

        if emit_string and not kwargs["geo"]:
            # The GeoParquet path converts inside its writer instead, after
            # geometries have been built from the working form.
            string_meta = out_meta.copy()
            string_meta.index = out_meta.index.astype("string")
            string_meta[partition_col] = string_meta[partition_col].astype("string")
            ddf = ddf.map_partitions(
                _cells_frame_to_string, indexer, partition_col, meta=string_meta
            )

        hist_spec = kwargs.get("hist_spec")
        write_schema = _build_write_schema(
            out=out,
            aggfuncs=aggfuncs,
            band_cols=band_cols,
            decimals=decimals,
            source_dtypes=source_dtypes,
            index_col=index_col,
            partition_col=partition_col,
            out_meta=out_meta,
            hist_spec=hist_spec,
            cell_type=output_cell_type,
        )
        if out == const.OutputSchema.HISTOGRAM and hist_spec is not None:
            hist_meta = {
                "mode": "binned" if hist_spec.binned else "categorical",
                "edges": list(hist_spec.edges) if hist_spec.edges else None,
                "width": hist_spec.width,
                "origin": hist_spec.origin,
                "weight": str(hist_spec.weight),
                "normalize": str(hist_spec.normalize),
            }
            existing_meta = write_schema.metadata or {}
            write_schema = write_schema.with_metadata(
                {
                    **existing_meta,
                    b"raster2dggs:histogram": json.dumps(hist_meta).encode("utf-8"),
                }
            )

        _write_output(
            ddf,
            output=output,
            geo=kwargs["geo"],
            compression=kwargs["compression"],
            partition_col=partition_col,
            overwrite=kwargs["overwrite"],
            write_schema=write_schema,
            indexer=indexer,
            cells_to_string=indexer.cells_to_string if emit_string else None,
        )

    LOGGER.debug("Stage 2 (aggregation) complete")
    return output


# Stage 1 runs each window in a worker process. Everything below exists to make
# that possible: a worker receives configuration, not objects, and looks its
# callables up by name.

# "fork" is unsafe here -- this process has already imported GDAL, PROJ and BLAS,
# and forking a process holding their locks can deadlock the child. Pinned rather
# than left to the platform default, which differs between Python versions.
_START_METHOD = "spawn" if sys.platform == "win32" else "forkserver"

# Floor for the per-worker GDAL block cache. GDAL's own default is a percentage
# of total RAM, applied per process, so N workers would each claim a full share.
_MIN_GDAL_CACHE_MB = 64


def _total_ram_bytes() -> int | None:
    """Physical RAM in bytes, or None if it cannot be determined."""
    try:
        return os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES")
    except (AttributeError, OSError, ValueError):
        pass
    if sys.platform == "win32":  # pragma: no cover - Windows only
        import ctypes

        class _MemoryStatusEx(ctypes.Structure):
            _fields_ = [
                ("dwLength", ctypes.c_uint32),
                ("dwMemoryLoad", ctypes.c_uint32),
                ("ullTotalPhys", ctypes.c_uint64),
                ("ullAvailPhys", ctypes.c_uint64),
                ("ullTotalPageFile", ctypes.c_uint64),
                ("ullAvailPageFile", ctypes.c_uint64),
                ("ullTotalVirtual", ctypes.c_uint64),
                ("ullAvailVirtual", ctypes.c_uint64),
                ("ullAvailExtendedVirtual", ctypes.c_uint64),
            ]

        status = _MemoryStatusEx()
        status.dwLength = ctypes.sizeof(status)
        if ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(status)):
            return status.ullTotalPhys
    return None


def _gdal_cache_budget_mb() -> int:
    """Total block cache to share between workers: a user-set GDAL_CACHEMAX if
    present, otherwise GDAL's own single-process default (5% of physical RAM).

    Computed here rather than asked of GDAL so that the osgeo bindings are not
    a runtime dependency (raster I/O goes through rasterio's libgdal, which
    needs no Python bindings). Parsing follows GDAL's rules for GDAL_CACHEMAX:
    a percentage of RAM, or a number read as MB when small and bytes when
    >= 100000. An unparseable value falls back to the default, as each worker's
    libgdal will likewise ignore it."""
    ram = _total_ram_bytes()
    value = os.environ.get("GDAL_CACHEMAX", "").strip()
    if value:
        try:
            if value.endswith("%"):
                if ram is not None:
                    pct = float(value[:-1])
                    return max(_MIN_GDAL_CACHE_MB, int(ram * pct / 100 / 2**20))
            else:
                n = int(value)
                return max(_MIN_GDAL_CACHE_MB, n if n < 100_000 else n // 2**20)
        except ValueError:
            pass
    if ram is not None:
        return max(_MIN_GDAL_CACHE_MB, int(ram * 0.05 / 2**20))
    return _MIN_GDAL_CACHE_MB


# Per-process Stage 1 state, populated once per worker. Worker callables must be
# importable by name, so they cannot close over this; a module global is the
# hand-off between the initialiser and the per-window function.
_WORKER: dict = {}


@dataclasses.dataclass(frozen=True)
class _ParquetWriter:
    """Write one window's result into the Stage 1 store.

    Stands in for a closure: a function defined inside ``initial_index`` has no
    importable name, so it cannot be sent to a worker process.
    """

    tmpdir: str
    partition_col: str
    compression: str

    def __call__(self, result, window) -> None:
        if result is None or (hasattr(result, "num_rows") and result.num_rows == 0):
            return
        with PROFILER.phase("stage1.parquet_write"):
            pq.write_to_dataset(
                result,
                root_path=self.tmpdir,
                partition_cols=[self.partition_col],
                basename_template=f"{window.col_off}.{window.row_off}." + "{i}.parquet",
                use_threads=False,  # one window per worker already
                # Overwrite files of the same name; ignore other existing files,
                # which allows an append workflow.
                existing_data_behavior="overwrite_or_ignore",
                compression=self.compression,
            )


def _setup_stage1_worker(cfg: dict) -> None:
    """Build this process's transfer context from picklable configuration.

    Called once per worker, and directly in-process when running inline, so both
    paths construct the context identically.
    """
    if cfg["gdal_cachemax_mb"]:
        os.environ["GDAL_CACHEMAX"] = str(cfg["gdal_cachemax_mb"])

    env = rio.Env()
    env.__enter__()
    src = rio.open(cfg["raster_input"], mode="r", sharing=False)
    indexer = idxfactory.indexer_instance(cfg["dggs"])
    write_result = _ParquetWriter(
        tmpdir=cfg["tmpdir"],
        partition_col=indexer.partition_col(cfg["parent_res"]),
        compression=cfg["compression"],
    )
    shared = dict(
        indexer=indexer,
        resolution=cfg["resolution"],
        parent_res=cfg["parent_res"],
        selected_labels=cfg["selected_labels"],
        selected_indices=cfg["selected_indices"],
        nodata_policy=cfg["nodata_policy"],
        emit_nodata_value=cfg["emit_nodata_value"],
        write_result=write_result,
    )

    da = None
    if cfg["transfer"] in const.OVERLAY_TRANSFER_KEYS:
        ctx = _OverlayIndexer(
            raster_input=cfg["raster_input"],
            op=cfg["op"],
            out=cfg["out"],
            min_valid_coverage=cfg["min_valid_coverage"],
            decimals=cfg["decimals"],
            hist_spec=cfg["hist_spec"],
            **cfg["overlay_shared_paths"],
            **shared,
        )
        func = ctx.process_window
        transformer = None
    else:
        da = rioxarray.open_rasterio(
            src,
            lock=dask.utils.SerializableLock(),
            masked=False,
            default_name=const.DEFAULT_NAME,
        ).chunk(**{"y": "auto", "x": "auto"})
        if "band" in da.dims and len(cfg["selected_indices"]) != src.count:
            if "band" in da.coords:
                da = da.sel(band=list(cfg["selected_indices"]))
            else:
                da = da.isel(band=[i - 1 for i in cfg["selected_indices"]])

        if cfg["transfer"] == const.Transfer.SAMPLE:
            transformer = pyproj.Transformer.from_crs(
                "EPSG:4326", src.crs, always_xy=True
            )
            ctx = _SampleIndexer(
                src=src,
                da=da,
                inverse_transformer=transformer,
                nodata=src.nodata,
                **shared,
            )
            func = {
                const.Interp.BILINEAR: ctx.process_bilinear,
                const.Interp.BICUBIC: ctx.process_bicubic,
                const.Interp.LANCZOS: ctx.process_lanczos,
            }.get(cfg["interp"], ctx.process_nn)
        else:
            transformer = pyproj.Transformer.from_crs(
                src.crs, "EPSG:4326", always_xy=True
            )
            ctx = _AssignCentersIndexer(
                da=da, nodata=src.nodata, transformer=transformer, **shared
            )
            func = ctx.process_window

    _WORKER.update(env=env, src=src, da=da, transformer=transformer, ctx=ctx, func=func)


def _init_stage1_worker(cfg: dict) -> None:
    """``ProcessPoolExecutor`` initialiser: one call per worker process."""
    PROFILER.reset(enabled=cfg["profile"])
    _setup_stage1_worker(cfg)
    # A pool worker is never told when the run ends, and tearing GDAL/PROJ
    # objects down at interpreter shutdown is what caused the silent crash
    # described in _close_stage1_worker. atexit runs early enough to avoid it.
    atexit.register(_close_stage1_worker)


def _run_stage1_window(window: tuple):
    """Index one window. Returns this worker's profile totals when profiling."""
    with PROFILER.phase("stage1.window_total"):
        _WORKER["func"](rio.windows.Window(*window))
    if PROFILER.enabled:
        return os.getpid(), PROFILER.snapshot()
    return None


def _close_stage1_worker() -> None:
    """Release this process's GDAL/PROJ objects.

    ``da`` and the transformer are held by ``ctx`` as dataclass fields and are
    not freed by reference counting alone if they are in a dask task-graph cycle.
    Dropping them explicitly while the dataset is still open tears the GDAL/PROJ
    objects down during normal execution rather than at interpreter shutdown,
    which causes a silent "Error in sys.excepthook" crash for non-WGS84 rasters.
    """
    if not _WORKER:
        return
    ctx = _WORKER.pop("ctx", None)
    da = _WORKER.pop("da", None)
    _WORKER.pop("transformer", None)
    _WORKER.pop("func", None)
    if da is not None:
        da.close()
    if hasattr(ctx, "close"):
        ctx.close()
    del ctx, da
    gc.collect()
    src = _WORKER.pop("src", None)
    if src is not None:
        src.close()
    env = _WORKER.pop("env", None)
    if env is not None:
        env.__exit__(None, None, None)
    _WORKER.clear()


def initial_index(
    dggs: str,
    raster_input: Path | str,
    output: Path,
    resolution: int,
    parent_res: None | int,
    bands: Sequence[int | str] | None = None,
    nodata_policy: str = "omit",
    emit_nodata_value: int | float | None = None,
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
    validate_config(
        kwargs.get("point"),
        kwargs.get("overlay"),
        kwargs.get("sample"),
        kwargs.get("hist_bins"),
        kwargs.get("hist_width"),
        kwargs.get("hist_weight"),
        kwargs.get("hist_normalize"),
    )
    internal = resolve_to_internal(
        kwargs.get("point"),
        kwargs.get("overlay"),
        kwargs.get("sample"),
    )
    kwargs = {**kwargs, **internal}
    kwargs["hist_spec"] = _build_histogram_spec(kwargs)

    indexer = idxfactory.indexer_instance(dggs)

    if (
        kwargs["transfer"] in {const.Transfer.SAMPLE, *const.OVERLAY_TRANSFER_KEYS}
        and not indexer.SUPPORTS_CELL_ENUMERATION
    ):
        raise click.UsageError(
            f"--transfer {kwargs['transfer']!r} requires spatial cell enumeration, "
            f"which is not supported by the {dggs!r} DGGS."
        )

    if (
        kwargs.get("cell_id", const.CellId.STRING) == const.CellId.UINT64
        and indexer.CELL_ARROW_TYPE == pa.string()
    ):
        raise click.UsageError(
            f"--cell-id uint64: the {dggs!r} DGGS has no integer cell form "
            f"(its cell IDs are strings such as geohashes or rHEALPix addresses), "
            f"or integer support has not been implemented for it yet."
        )

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
                            ) from e
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

                windows = [window for _, window in src.block_windows()]
                LOGGER.debug(
                    "%d windows",
                    len(windows),
                )

                # Context for --profile: timings are only interpretable
                # against the shape of the work they measure.
                PROFILER.note("windows", len(windows))
                PROFILER.note("bands", len(selected_indices))
                PROFILER.note("raster_size", f"{src.width}x{src.height}")
                PROFILER.note(
                    "block_shape", f"{src.block_shapes[0][1]}x{src.block_shapes[0][0]}"
                )
                PROFILER.note("internally_tiled", bool(src.profile.get("tiled", False)))
                PROFILER.note("processes", kwargs["processes"])

                selected_labels = tuple([labels_by_index[i] for i in selected_indices])
                kwargs["source_pixel_dtypes"] = {
                    label: np.dtype(src.dtypes[idx - 1])
                    for idx, label in zip(
                        selected_indices, selected_labels, strict=True
                    )
                }
                processes = max(1, int(kwargs["processes"]))
                cfg = {
                    "raster_input": str(raster_input),
                    "dggs": dggs,
                    "resolution": resolution,
                    "parent_res": parent_res,
                    "selected_indices": tuple(selected_indices),
                    "selected_labels": selected_labels,
                    "nodata_policy": nodata_policy,
                    "emit_nodata_value": emit_nodata_value,
                    "transfer": kwargs["transfer"],
                    "interp": kwargs.get("interp", const.Interp.NN),
                    "op": kwargs.get("op"),
                    "out": kwargs.get("out"),
                    "min_valid_coverage": kwargs.get("valid_coverage_threshold", 0.0),
                    "decimals": kwargs.get("decimals"),
                    "hist_spec": kwargs.get("hist_spec"),
                    "compression": kwargs["compression"],
                    "tmpdir": tmpdir,
                    "profile": PROFILER.enabled,
                    # GDAL sizes its block cache as a share of total RAM per
                    # process, so N workers would each claim a full share.
                    "gdal_cachemax_mb": max(
                        _MIN_GDAL_CACHE_MB, _gdal_cache_budget_mb() // processes
                    ),
                    "overlay_shared_paths": {},
                }

                # --overlay may need raster-sized weights and validity rasters.
                # Build them once here and let every worker attach to them; each
                # worker building its own would mean N full-raster reads, writes
                # and (for a geographic CRS) N passes of a per-row Python loop.
                shared_overlay = None
                if kwargs["transfer"] in const.OVERLAY_TRANSFER_KEYS:
                    shared_overlay = _OverlayIndexer(
                        raster_input=cfg["raster_input"],
                        indexer=indexer,
                        resolution=resolution,
                        parent_res=parent_res,
                        selected_labels=selected_labels,
                        selected_indices=cfg["selected_indices"],
                        nodata_policy=nodata_policy,
                        emit_nodata_value=emit_nodata_value,
                        write_result=None,
                        op=kwargs["op"],
                        out=kwargs["out"],
                        min_valid_coverage=cfg["min_valid_coverage"],
                        decimals=cfg["decimals"],
                        hist_spec=cfg["hist_spec"],
                    )
                    cfg["overlay_shared_paths"] = shared_overlay.shared_temp_paths()

                window_args = [
                    (w.col_off, w.row_off, w.width, w.height) for w in windows
                ]
                try:
                    with PROFILER.phase("stage1.wall"), tqdm(
                        total=len(windows), desc="Raster windows"
                    ) as pbar:
                        if processes > 1:
                            # One snapshot per worker: a worker's totals only
                            # grow, so the last one it sends is its total.
                            latest: dict[int, dict] = {}
                            with ProcessPoolExecutor(
                                max_workers=processes,
                                initializer=_init_stage1_worker,
                                initargs=(cfg,),
                                mp_context=multiprocessing.get_context(_START_METHOD),
                            ) as executor:
                                for result in executor.map(
                                    _run_stage1_window,
                                    window_args,
                                    chunksize=max(
                                        1, len(window_args) // (processes * 4)
                                    ),
                                ):
                                    if result is not None:
                                        latest[result[0]] = result[1]
                                    pbar.update(1)
                            for snapshot in latest.values():
                                PROFILER.merge(snapshot)
                        else:
                            # Inline: a pool of one would pay for machinery it
                            # cannot use.
                            _setup_stage1_worker(cfg)
                            try:
                                for window_arg in window_args:
                                    _run_stage1_window(window_arg)
                                    pbar.update(1)
                            finally:
                                _close_stage1_worker()
                finally:
                    if shared_overlay is not None:
                        shared_overlay.close()
                    del shared_overlay
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
