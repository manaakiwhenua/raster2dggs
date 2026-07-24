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
from rasterio.transform import rowcol

import raster2dggs.constants as const
import raster2dggs.histogram as histogram
import raster2dggs.indexerfactory as idxfactory

from raster2dggs.interfaces import IRasterIndexer
from raster2dggs.indexers.rasterindexer import _is_nan

LOGGER = logging.getLogger(__name__)
click_log.basic_config(LOGGER)

from raster2dggs.transfers.assign_centers import _AssignCentersIndexer
from raster2dggs.transfers.interpolation import _SampleIndexer
from raster2dggs.transfers.overlay import _OverlayIndexer


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
    point: Optional[str],
    overlay: Optional[str],
    sample: Optional[str],
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


def _build_histogram_spec(kwargs: dict) -> Optional[histogram.HistogramSpec]:
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
    point: Optional[str],
    overlay: Optional[str],
    sample: Optional[str],
    hist_bins: Optional[Sequence[float]] = None,
    hist_width: Optional[float] = None,
    hist_weight: Optional[str] = None,
    hist_normalize: Optional[str] = None,
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
    threads: int,
    aggfuncs: List[Tuple[str, Union[str, Callable]]],
    decimals: int,
    overwrite: bool,
    compact: bool,
    geo: str,
    point: Optional[str] = None,
    overlay: Optional[str] = None,
    sample: Optional[str] = None,
    valid_coverage_threshold: float = 0.0,
    hist_bins: Optional[Tuple[float, ...]] = None,
    hist_width: Optional[float] = None,
    hist_origin: float = 0.0,
    hist_weight: str = "count",
    hist_normalize: str = "none",
) -> dict:
    return {
        "compression": compression,
        "threads": threads,
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
                partition_col: pd.Series([], dtype="string"),
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
                partition_col: pd.Series([], dtype="string"),
                **{c: pd.Series([], dtype="object") for c in band_cols},
            }
        )
        tqdm_label = (
            "Collecting"
            if out != const.OutputSchema.VALUE
            else f"Aggregating{'/compacting' if compact else ''}"
        )
    out_meta.index = pd.Index([], name=index_col, dtype="string")
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
    hist_spec: Optional[histogram.HistogramSpec] = None,
) -> pa.Schema:
    common_fields = [
        pa.field(index_col, pa.string()),
        pa.field(partition_col, pa.string()),
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
    return pa.Schema.from_pandas(out_meta, preserve_index=True)


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

    # Don't let partition schema be inferred; e.g. geohash levels can be
    # inferred variously as int or string.
    part_schema = pa.schema([(partition_col, pa.string())])
    ddf = dd.read_parquet(
        pq_input,
        engine="pyarrow",
        aggregate_files=True,
        dataset={"partitioning": ds.partitioning(part_schema, flavor="hive")},
    )
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
    )

    with TqdmCallback(desc=tqdm_label):
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

        ddf = ddf.map_partitions(mp_func, *mp_args, meta=out_meta)

        if kwargs["compact"]:
            ddf = ddf.map_partitions(
                indexer.compaction, resolution, parent_res, meta=out_meta
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
        )

    LOGGER.debug("Stage 2 (aggregation) complete")
    return output


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
                if kwargs["transfer"] == const.Transfer.SAMPLE:
                    inverse_transformer = pyproj.Transformer.from_crs(
                        "EPSG:4326", src.crs, always_xy=True
                    )
                    LOGGER.debug("Inverse transformer: EPSG:4326 → %s", src.crs)
                else:
                    inverse_transformer = None

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
                kwargs["source_pixel_dtypes"] = {
                    label: np.dtype(src.dtypes[idx - 1])
                    for idx, label in zip(selected_indices, selected_labels)
                }
                compression = kwargs["compression"]
                nodata = src.nodata

                def _write_result(result, window):
                    if result is None or (
                        hasattr(result, "num_rows") and result.num_rows == 0
                    ):
                        return
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

                if kwargs["transfer"] == const.Transfer.SAMPLE:
                    ctx = _SampleIndexer(
                        src=src,
                        da=da,
                        inverse_transformer=inverse_transformer,
                        nodata=nodata,
                        indexer=indexer,
                        resolution=resolution,
                        parent_res=parent_res,
                        selected_labels=selected_labels,
                        nodata_policy=nodata_policy,
                        emit_nodata_value=emit_nodata_value,
                        write_result=_write_result,
                    )
                    interp = kwargs.get("interp", const.Interp.NN)
                    if interp == const.Interp.BILINEAR:
                        stage1_func = ctx.process_bilinear
                    elif interp == const.Interp.BICUBIC:
                        stage1_func = ctx.process_bicubic
                    elif interp == const.Interp.LANCZOS:
                        stage1_func = ctx.process_lanczos
                    else:
                        stage1_func = ctx.process_nn
                elif kwargs["transfer"] in const.OVERLAY_TRANSFER_KEYS:
                    ctx = _OverlayIndexer(
                        raster_input=str(raster_input),
                        indexer=indexer,
                        resolution=resolution,
                        parent_res=parent_res,
                        selected_labels=selected_labels,
                        selected_indices=tuple(selected_indices),
                        nodata_policy=nodata_policy,
                        emit_nodata_value=emit_nodata_value,
                        write_result=_write_result,
                        op=kwargs["op"],
                        out=kwargs["out"],
                        min_valid_coverage=kwargs.get("valid_coverage_threshold", 0.0),
                        decimals=kwargs.get("decimals"),
                        hist_spec=kwargs.get("hist_spec"),
                    )
                    stage1_func = ctx.process_window
                else:
                    ctx = _AssignCentersIndexer(
                        da=da,
                        indexer=indexer,
                        resolution=resolution,
                        parent_res=parent_res,
                        nodata=nodata,
                        selected_labels=selected_labels,
                        nodata_policy=nodata_policy,
                        emit_nodata_value=emit_nodata_value,
                        transformer=transformer,
                        write_result=_write_result,
                    )
                    stage1_func = ctx.process_window
                try:
                    with ThreadPoolExecutor(
                        max_workers=kwargs["threads"]
                    ) as executor, tqdm(
                        total=len(windows), desc="Raster windows"
                    ) as pbar:
                        for _ in executor.map(stage1_func, windows, chunksize=1):
                            pbar.update(1)
                finally:
                    da.close()
                    # da and transformer are held by ctx as dataclass fields and
                    # won't be freed by reference counting alone if they're in a
                    # dask task-graph cycle.  Explicitly delete the locals and ctx
                    # now, while still inside rio.open(), so the GDAL/PROJ objects
                    # they hold are torn down during normal execution rather than at
                    # interpreter shutdown (which causes a silent "Error in
                    # sys.excepthook" crash for non-WGS84 rasters).
                    del da, transformer, ctx, stage1_func
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
