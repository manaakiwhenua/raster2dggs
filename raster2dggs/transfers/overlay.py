"""
Raster overlay context for --transfer overlay_weighted, overlay_mode, mass_preserve.

_OverlayIndexer holds all shared state (raster path, indexer config) and exposes
process_window as a bound method callable by ThreadPoolExecutor.map.

exactextract reads from the full raster path for every polygon batch, so a cell
whose polygon spans multiple raster windows always receives correct combined stats.
Window boundaries affect only which batch discovers each cell; Stage 2 dedup
(parent_groupby_nn) handles any duplicates.
"""

from __future__ import annotations

import dataclasses
import logging
import math
import os
import tempfile
import warnings
from typing import Any, Callable, Optional

import antimeridian
from exactextract import exact_extract
import geopandas as gpd
import numpy as np
import pandas as pd
import pyarrow as pa
import pyproj
import rasterio as rio
from rasterio.warp import transform_bounds, transform as warp_transform
from shapely.geometry import Polygon, shape

import raster2dggs.constants as const
from raster2dggs.interfaces import IRasterIndexer

LOGGER = logging.getLogger(__name__)

_FRAC_STRUCT_TYPE = pa.struct(
    [
        pa.field("classes", pa.list_(pa.int64())),
        pa.field("fractions", pa.list_(pa.float64())),
    ]
)


def _frac_pairs(frac_arr, unique_arr, scale=1.0, decimals=None):
    """Return {"classes": [...], "fractions": [...]} sorted by class, or None if no valid pixels.

    scale: multiply each fraction by this value so fractions represent the fraction of the
    total cell area covered by each class, not just the fraction of valid pixels.
    """
    if frac_arr is None or unique_arr is None or len(frac_arr) == 0:
        return None
    if scale <= 0.0:
        return None
    pairs = sorted(zip(unique_arr.tolist(), frac_arr.tolist()))
    fracs = [v * scale for _, v in pairs]
    if decimals is not None:
        fracs = [round(f, decimals) for f in fracs]
    return {"classes": [k for k, _ in pairs], "fractions": fracs}


def _build_histogram(arr, decimals=None) -> Optional[dict]:
    """Return {"values": [...], "counts": [...]} from a pixel-value array, or None if empty."""
    if arr is None or len(arr) == 0:
        return None
    if decimals is not None:
        arr = np.round(arr, decimals)
    unique, counts = np.unique(arr, return_counts=True)
    return {"values": unique.tolist(), "counts": counts.astype(np.int64).tolist()}


def _pa_elem_type(numpy_dtype_str: str, decimals) -> pa.DataType:
    if decimals is not None and decimals <= 0:
        return pa.int64()
    if decimals is not None:
        return pa.float64()
    return pa.from_numpy_dtype(numpy_dtype_str)


def _build_collect_table(
    result_df: pd.DataFrame,
    band_cols: list,
    src_dtypes: tuple,
    selected_indices: tuple,
    out: str,
    decimals,
) -> pa.Table:
    """Build a PyArrow Table for --overlay --list or --overlay --histogram output."""
    arrays = {}
    for col in result_df.columns:
        if col not in band_cols:
            arrays[col] = pa.array(result_df[col].tolist())
    for idx, col in zip(selected_indices, band_cols):
        elem_type = _pa_elem_type(src_dtypes[idx - 1], decimals)
        if out == const.OutputSchema.LIST:
            arrays[col] = pa.array(result_df[col].tolist(), type=pa.list_(elem_type))
        else:
            hist_type = pa.struct(
                [
                    pa.field("values", pa.list_(elem_type)),
                    pa.field("counts", pa.list_(pa.int64())),
                ]
            )
            arrays[col] = pa.array(result_df[col].tolist(), type=hist_type)
    return pa.table(arrays)


def _build_frac_table(result_df: pd.DataFrame, band_cols: list) -> pa.Table:
    """Build a PyArrow Table with struct<classes, fractions> columns for fraction_cover output."""
    arrays = {}
    for col in result_df.columns:
        if col in band_cols:
            arrays[col] = pa.array(result_df[col].tolist(), type=_FRAC_STRUCT_TYPE)
        else:
            arrays[col] = pa.array(result_df[col].tolist())
    return pa.table(arrays)


def _fix_antimeridian(poly):
    """Return poly unchanged, or a split MultiPolygon if it crosses ±180°.

    Shapely polygons for DGGS cells that straddle the antimeridian mix +179° and
    -179° coordinates, making the polygon appear ~360° wide.  antimeridian.fix_shape
    splits it cleanly at ±180°; exactextract handles MultiPolygon as a single feature.
    """
    if poly.bounds[2] - poly.bounds[0] <= 180:
        return poly
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        fixed = antimeridian.fix_shape(poly, fix_winding=True)
    return shape(fixed)


@dataclasses.dataclass(repr=False)
class _OverlayIndexer:
    """Shared context for --transfer overlay_weighted / overlay_mode / mass_preserve.

    Instantiate once; pass ctx.process_window to ThreadPoolExecutor.map.
    """

    raster_input: str
    indexer: IRasterIndexer
    resolution: int
    parent_res: int
    selected_labels: tuple
    selected_indices: tuple
    nodata_policy: str
    emit_nodata_value: Optional[Any]
    write_result: Callable
    op: const.Op
    out: const.OutputSchema = const.OutputSchema.VALUE
    min_valid_coverage: float = 0.0
    decimals: Optional[int] = None

    def __post_init__(self):
        # mass_preserve (sum) must not filter by coverage — partial sums are correct
        # values representing partial mass, not missing data. Filtering would break
        # the conservation property the transfer is designed to enforce.
        self._apply_coverage_threshold = (
            self.min_valid_coverage > 0.0 and self.op != const.Op.SUM
        )
        if self.min_valid_coverage > 0.0 and self.op == const.Op.SUM:
            LOGGER.warning(
                "--valid-coverage-threshold has no effect with --transfer mass_preserve "
                "(partial sums are correct values; filtering them would break mass conservation)"
            )

        self._geodesic_weights_path = None
        with rio.open(self.raster_input, sharing=False) as src:
            self._src_crs = src.crs
            self._src_transform = src.transform
            self._src_band_count = src.count
            self._src_dtypes = src.dtypes

            if (
                self.op == const.Op.MEAN and src.crs.is_geographic
            ) or self.op == const.Op.WSUM:
                # Pixel-area weights raster for exactextract weighted_mean / weighted_sum.
                # For geographic CRS: geodesic area varies by row (latitude); use pyproj.
                # For projected CRS (wsum only): pixel area is constant from the transform.
                T = src.transform
                if src.crs.is_geographic:
                    geod = pyproj.CRS(src.crs.to_wkt()).get_geod()
                    areas = np.empty((src.height, src.width), dtype=np.float64)
                    lon_left, lon_right = T.c, T.c + T.a
                    for r in range(src.height):
                        lat_top = T.f + r * T.e
                        lat_bot = T.f + (r + 1) * T.e
                        area, _ = geod.polygon_area_perimeter(
                            [lon_left, lon_right, lon_right, lon_left],
                            [lat_top, lat_top, lat_bot, lat_bot],
                        )
                        areas[r, :] = abs(area)
                else:
                    areas = np.full(
                        (src.height, src.width),
                        abs(T.a * T.e),
                        dtype=np.float64,
                    )
                wgt_profile = {
                    "driver": "GTiff",
                    "height": src.height,
                    "width": src.width,
                    "count": 1,
                    "dtype": "float64",
                    "crs": src.crs,
                    "transform": src.transform,
                    "nodata": None,
                }
                with tempfile.NamedTemporaryFile(suffix=".tif", delete=False) as tmp:
                    self._geodesic_weights_path = tmp.name
                with rio.open(self._geodesic_weights_path, "w", **wgt_profile) as dst:
                    dst.write(areas[np.newaxis])
                LOGGER.debug(
                    "Pixel area weights computed for %s (%.0f–%.0f units²/pixel)",
                    src.crs.to_string(),
                    areas.min(),
                    areas.max(),
                )

            if self._apply_coverage_threshold or self.op == const.Op.FRAC:
                # Densified boundary polygon of the raster footprint in WGS84.
                # Sampling n_edge points per edge (in pixel space) and reprojecting
                # them all correctly captures curved boundaries for projected CRS
                # rasters (e.g. UTM). Using box(*transform_bounds(...)) — 4 corners
                # only — overestimates the footprint near edge midpoints: for UTM the
                # bottom boundary bows south at the corners but the bbox extends to
                # that latitude everywhere, so cells just south of the bottom-centre
                # appear inside the bbox and receive raster_fracs ≈ 1.0 even when
                # they overlap almost no actual raster pixels.
                n_edge = 100
                h, w = src.height, src.width
                perim_row = np.concatenate(
                    [
                        np.full(n_edge, h),  # bottom: left → right
                        np.linspace(h, 0, n_edge),  # right:  bottom → top
                        np.zeros(n_edge),  # top:    right → left
                        np.linspace(0, h, n_edge),  # left:   top → bottom
                    ]
                )
                perim_col = np.concatenate(
                    [
                        np.linspace(0, w, n_edge),  # bottom
                        np.full(n_edge, w),  # right
                        np.linspace(w, 0, n_edge),  # top
                        np.zeros(n_edge),  # left
                    ]
                )
                xs, ys = rio.transform.xy(
                    src.transform, perim_row, perim_col, offset="ul"
                )
                lons, lats = warp_transform(src.crs, "EPSG:4326", list(xs), list(ys))
                self._raster_footprint_wgs84 = Polygon(zip(lons, lats))

                # Build a per-band binary validity mask (1.0=valid, 0.0=nodata) written
                # to a real temp file so exactextract can compute mean(mask) = valid data
                # fraction within the raster-overlapping area.  Multiplied by the
                # raster-extent coverage fraction in process_window to give the true
                # valid fraction relative to the full cell area.
                # The mask has no nodata value so 0.0 values are treated as real data.
                # A real file (not /vsimem/) avoids spurious GDAL "ERROR 4: No such file
                # or directory" messages from PAM auxiliary-file lookups on vsimem paths.
                data = src.read(masked=True)
                mask_profile = src.profile.copy()
                mask_profile.update(dtype="float32", nodata=None)
                with tempfile.NamedTemporaryFile(suffix=".tif", delete=False) as tmp:
                    self._coverage_mask_path = tmp.name
                # data.mask is scalar False when the source has no nodata value;
                # broadcast to full (bands, height, width) shape before writing.
                validity = (~np.broadcast_to(data.mask, data.shape)).astype(np.float32)
                with rio.open(self._coverage_mask_path, "w", **mask_profile) as dst:
                    dst.write(validity)
            else:
                self._raster_footprint_wgs84 = None
                self._coverage_mask_path = None

        # exactextract column naming:
        #   unweighted: "{op}" (single-band) or "band_{i}_{op}" (multi-band)
        #   weighted:   "weighted_{op}" (single-band) or "band_{i}_weight_weighted_{op}" (multi-band)
        if self._geodesic_weights_path is not None:
            weighted_agg = (
                "weighted_sum" if self.op == const.Op.WSUM else "weighted_mean"
            )
            if self._src_band_count == 1:
                self._col_rename = {weighted_agg: self.selected_labels[0]}
            else:
                self._col_rename = {
                    f"band_{idx}_weight_{weighted_agg}": label
                    for idx, label in zip(self.selected_indices, self.selected_labels)
                }
        elif self._src_band_count == 1:
            self._col_rename = {self.op: self.selected_labels[0]}
        else:
            self._col_rename = {
                f"band_{idx}_{self.op}": label
                for idx, label in zip(self.selected_indices, self.selected_labels)
            }

    def _cleanup_temp_files(self):
        for path_attr in ("_coverage_mask_path", "_geodesic_weights_path"):
            path = getattr(self, path_attr, None)
            if path is not None:
                try:
                    os.unlink(path)
                except OSError:
                    pass  # already deleted or interpreter shutting down — nothing to do
                finally:
                    setattr(self, path_attr, None)

    def close(self):
        self._cleanup_temp_files()

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass

    def process_window(self, window):
        """Compute overlay stats for all DGGS cells overlapping this raster window."""
        bounds = rio.windows.bounds(window, self._src_transform)
        min_lon, min_lat, max_lon, max_lat = transform_bounds(
            self._src_crs, "EPSG:4326", *bounds
        )

        # Expand so that cells touching the window edge are captured.
        # cells_in_bbox is centre-based: a cell whose polygon overlaps this window
        # but whose centre is just outside would be missed without a buffer.
        # The buffer must be at least one cell circumradius in all directions.
        # For H3 hexagons, circumradius R ≈ 0.62 * sqrt(area_m2); sqrt(area_m2)
        # gives ~1.6 R — a comfortable margin.
        # Latitude degrees are ~constant (111 195 m/°); longitude degrees shrink
        # by cos(lat), so we correct the longitude pad to keep the buffer uniform
        # in metres.  Clamped to cos(lat) ≥ 0.1 (≈ 84° lat) to avoid blowup at poles.
        # False-positive cells (centres in-range but polygon outside raster) return
        # NaN from exactextract and are dropped by the nodata policy.
        cx = (min_lon + max_lon) / 2
        cy = (min_lat + max_lat) / 2
        area_m2 = self.indexer.cell_area_m2(self.resolution, cy, cx)
        lat_pad = math.sqrt(area_m2) / const.WGS84_APPROX_DISTANCE_DEG_M
        lon_pad = lat_pad / max(math.cos(math.radians(cy)), 0.1)
        min_lon = max(-180.0, min_lon - lon_pad)
        max_lon = min(180.0, max_lon + lon_pad)
        min_lat = max(-90.0, min_lat - lat_pad)
        max_lat = min(90.0, max_lat + lat_pad)

        cells = list(
            self.indexer.cells_in_bbox(
                min_lon, min_lat, max_lon, max_lat, self.resolution
            )
        )
        if not cells:
            return None

        polygons = [_fix_antimeridian(self.indexer.cell_to_polygon(c)) for c in cells]
        # WGS84 GDF for VCT shapely area computation (raster_fracs).
        gdf_wgs84 = gpd.GeoDataFrame(
            {"_cell_id": cells}, geometry=polygons, crs="EPSG:4326"
        )
        # exactextract does not reliably reproject features to the raster CRS itself;
        # reproject explicitly so intersections are computed in the correct coordinate space.
        gdf = gdf_wgs84.to_crs(self._src_crs)

        is_frac = self.op == const.Op.FRAC
        is_collect = self.op == const.Op.VALUES
        is_geodesic = self._geodesic_weights_path is not None
        weighted_agg = "weighted_sum" if self.op == const.Op.WSUM else "weighted_mean"
        if is_frac:
            main_ops = ["frac", "unique"]
        elif is_geodesic:
            main_ops = [weighted_agg]
        else:
            main_ops = [str(self.op)]
        if is_geodesic:
            result_df = exact_extract(
                self.raster_input,
                gdf,
                main_ops,
                weights=self._geodesic_weights_path,
                include_cols="_cell_id",
                output="pandas",
            )
        else:
            result_df = exact_extract(
                self.raster_input,
                gdf,
                main_ops,
                include_cols="_cell_id",
                output="pandas",
            )

        valid_frac_by_band = {}
        if is_frac or self._apply_coverage_threshold:
            # mean(mask) = valid_frac within the raster-overlapping area, scaled by
            # raster_frac so area outside the raster also counts against coverage.
            # For frac semantics: scales per-class fractions so they represent fraction
            # of the total cell area, not fraction of valid pixels (which would always
            # sum to 1.0 regardless of how much of the cell is nodata or outside raster).
            # For threshold: used to null cells below min_valid_coverage.
            cov_df = exact_extract(
                self._coverage_mask_path,
                gdf,
                ["mean"],
                include_cols="_cell_id",
                output="pandas",
            ).set_index("_cell_id")

            raster_fracs = pd.Series(
                [
                    (
                        min(
                            1.0,
                            poly.intersection(self._raster_footprint_wgs84).area
                            / poly.area,
                        )
                        if poly.area > 0
                        else 0.0
                    )
                    for poly in gdf_wgs84.geometry
                ],
                index=pd.Index(cells, name="_cell_id"),
            )

            result_df = result_df.set_index("_cell_id")
            for idx in self.selected_indices:
                if self._src_band_count == 1:
                    vf = cov_df["mean"] * raster_fracs
                    main_col = weighted_agg if is_geodesic else self.op
                    unique_col = "unique"
                else:
                    vf = cov_df[f"band_{idx}_mean"] * raster_fracs
                    main_col = (
                        f"band_{idx}_weight_{weighted_agg}"
                        if is_geodesic
                        else f"band_{idx}_{self.op}"
                    )
                    unique_col = f"band_{idx}_unique"
                valid_frac_by_band[idx] = vf
                if self._apply_coverage_threshold:
                    result_df[main_col] = result_df[main_col].where(
                        vf >= self.min_valid_coverage, other=None
                    )
                    if is_frac:
                        result_df[unique_col] = result_df[unique_col].where(
                            vf >= self.min_valid_coverage, other=None
                        )
            result_df = result_df.reset_index()

        band_cols = list(self.selected_labels)

        if is_frac:
            # Merge parallel frac/unique arrays into sorted struct pairs, scaling
            # each fraction by valid_frac so fractions represent fraction of the
            # total cell area covered by each class.
            for idx, label in zip(self.selected_indices, self.selected_labels):
                fc = "frac" if self._src_band_count == 1 else f"band_{idx}_frac"
                uc = "unique" if self._src_band_count == 1 else f"band_{idx}_unique"
                vf_values = (
                    valid_frac_by_band[idx].reindex(result_df["_cell_id"]).values
                )
                result_df[label] = [
                    _frac_pairs(f, u, scale=s, decimals=self.decimals)
                    for f, u, s in zip(result_df[fc], result_df[uc], vf_values)
                ]
            result_df = result_df[["_cell_id"] + band_cols]

            nd_mask = (
                result_df[band_cols]
                .apply(
                    lambda col: col.map(lambda x: x is None or len(x["classes"]) == 0)
                )
                .any(axis=1)
            )
        elif is_collect:
            for idx, label in zip(self.selected_indices, self.selected_labels):
                vc = "values" if self._src_band_count == 1 else f"band_{idx}_values"
                raw = result_df[vc]
                if self.out == const.OutputSchema.LIST:
                    result_df[label] = [
                        (
                            (
                                sorted(round(v, self.decimals) for v in arr.tolist())
                                if self.decimals is not None
                                else sorted(arr.tolist())
                            )
                            if arr is not None and len(arr) > 0
                            else None
                        )
                        for arr in raw
                    ]
                else:
                    result_df[label] = [
                        _build_histogram(arr, self.decimals) for arr in raw
                    ]
            result_df = result_df[["_cell_id"] + band_cols]

            nd_mask = (
                result_df[band_cols]
                .apply(lambda col: col.map(lambda x: x is None or len(x) == 0))
                .any(axis=1)
            )
        else:
            keep = ["_cell_id"] + [
                c for c in self._col_rename if c in result_df.columns
            ]
            result_df = result_df[keep].rename(columns=self._col_rename)

            nd_mask = result_df[band_cols].isna().any(axis=1)

        if self.nodata_policy.lower() == "omit":
            result_df = result_df[~nd_mask].reset_index(drop=True)
        else:
            if is_frac or is_collect:
                # No scalar fill value makes sense for struct/list columns; emit None.
                for col in band_cols:
                    result_df.loc[nd_mask, col] = None
            else:
                fill = (
                    self.emit_nodata_value
                    if self.emit_nodata_value is not None
                    else np.nan
                )
                for col in band_cols:
                    result_df.loc[nd_mask, col] = np.nan if pd.isna(fill) else fill

        if result_df.empty:
            return None

        index_col = self.indexer.index_col(self.resolution)
        partition_col = self.indexer.partition_col(self.parent_res)
        result_cells = result_df["_cell_id"].tolist()
        result_df[index_col] = result_cells
        result_df[partition_col] = list(
            self.indexer.single_parent_cells(result_cells, self.parent_res)
        )
        result_df = result_df.drop(columns=["_cell_id"])

        if is_frac:
            table = _build_frac_table(result_df, band_cols)
        elif is_collect:
            table = _build_collect_table(
                result_df,
                band_cols,
                self._src_dtypes,
                self.selected_indices,
                self.out,
                self.decimals,
            )
        else:
            table = pa.Table.from_pandas(result_df, preserve_index=False)
        self.write_result(table, window)
        return None
