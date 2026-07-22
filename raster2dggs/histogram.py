"""
Shared histogram building for --point histogram and --overlay histogram.

Both code paths collect per-cell contributing pixel values (optionally with
per-pixel weights, e.g. overlap area) and need identical binning/weighting/
normalization semantics; this module is the single implementation both call
into, so a change here (or a bug fix) applies uniformly to both.

Output shape:
  - Categorical (no binning): {"values": [...], <weight_field>: [...]} --
    exact pixel values paired with a summed weight per value.
  - Binned: {"left": [...], "right": [...], <weight_field>: [...]} -- three
    parallel lists, all indexed the same way, so bin i spans [left[i],
    right[i]) and has weight <weight_field>[i]. Every bin is half-open
    [left, right), except the final bin of an explicit --hist-bins run,
    which is also right-closed (matching numpy.histogram's convention).
    This closedness rule is fixed for every bin, so it isn't stored
    per-bin -- see the --hist-bins/--hist-width CLI help for the
    definitive statement of it.

<weight_field> (see weight_field_name) is named for what the numbers
represent: a plain pixel count is "counts", but an area sum or a normalized
fraction is named accordingly (e.g. "area", "area_frac") since those numbers
aren't counts.
"""

from __future__ import annotations

import dataclasses
from typing import Optional, Sequence, Union

import numpy as np
import pyarrow as pa

import raster2dggs.constants as const


@dataclasses.dataclass(frozen=True)
class HistogramSpec:
    """Primitives-only so it tokenizes cleanly across a dask task graph."""

    edges: Optional[tuple] = None  # explicit ascending bin edges
    width: Optional[float] = None  # uniform bin width
    origin: float = 0.0  # anchor for width-mode bins
    weight: str = const.HistWeight.COUNT
    normalize: str = const.HistNormalize.NONE

    @property
    def binned(self) -> bool:
        return self.edges is not None or self.width is not None


_WEIGHT_FIELD_NAMES = {
    (const.HistWeight.COUNT, const.HistNormalize.NONE): "counts",
    (const.HistWeight.COUNT, const.HistNormalize.VALID_OVERLAP): "count_frac",
    (const.HistWeight.AREA, const.HistNormalize.NONE): "area",
    (const.HistWeight.AREA, const.HistNormalize.CELL_AREA): "area_frac",
    (const.HistWeight.AREA, const.HistNormalize.VALID_OVERLAP): "area_share",
}


def weight_field_name(spec: Optional[HistogramSpec]) -> str:
    """Name of the per-bin weight field for this spec's (weight, normalize)
    combination -- chosen so the field describes what the numbers are (a
    plain pixel count, an area in m^2, or a fraction of some denominator)."""
    spec = spec or HistogramSpec()
    key = (spec.weight, spec.normalize)
    if key not in _WEIGHT_FIELD_NAMES:
        raise ValueError(
            f"--hist-weight {spec.weight} with --hist-normalize {spec.normalize} "
            "is not a supported combination"
        )
    return _WEIGHT_FIELD_NAMES[key]


def build_histogram(
    values: Sequence[float],
    weights: Optional[Sequence[float]] = None,
    spec: Optional[HistogramSpec] = None,
    decimals: Optional[int] = None,
    cell_area: Optional[float] = None,
) -> Optional[dict]:
    """
    Build a histogram from contributing pixel values (and optional per-pixel
    weights). See module docstring for the returned shape.

    - spec is None or categorical (`not spec.binned`): an exact-value
      histogram -- unique (optionally decimals-rounded) values, each paired
      with its summed weight (1 per pixel when weights is None).
    - spec.binned: bins values into `spec.edges` (explicit, out-of-range
      values dropped) or uniform `spec.width`/`spec.origin` bins (unbounded
      -- every finite value falls in some bin). Only non-empty bins are
      reported (sparse).

    Normalization (spec.normalize):
      - NONE: raw summed weight per bin/value.
      - VALID_OVERLAP: divide by the total weight of all valid (non-NaN)
        input values, computed *before* any out-of-range dropping -- so a
        set of bins can sum to < 1 if values were dropped as out-of-range.
      - CELL_AREA: divide by `cell_area` (caller-supplied; required if used).

    Returns None if there is nothing to report.
    """
    if values is None or len(values) == 0:
        return None
    arr = np.asarray(values, dtype=np.float64)
    valid = ~np.isnan(arr)
    if not valid.any():
        return None
    arr = arr[valid]
    w = (
        np.ones_like(arr)
        if weights is None
        else np.asarray(weights, dtype=np.float64)[valid]
    )
    total_valid_weight = float(w.sum())

    spec = spec or HistogramSpec()
    field = weight_field_name(spec)

    if not spec.binned:
        keys, out_weight = _categorical(arr, w, decimals)
        if len(keys) == 0:
            return None
        bins_payload = {"values": keys}
    else:
        if spec.edges is not None:
            lefts, rights, out_weight = _binned_explicit(arr, w, spec.edges)
        else:
            lefts, rights, out_weight = _binned_uniform(arr, w, spec.width, spec.origin)
        if len(lefts) == 0:
            return None
        bins_payload = {"left": lefts, "right": rights}

    if spec.normalize == const.HistNormalize.VALID_OVERLAP:
        if total_valid_weight <= 0:
            return None
        out_weight = out_weight / total_valid_weight
    elif spec.normalize == const.HistNormalize.CELL_AREA:
        if not cell_area or cell_area <= 0:
            return None
        out_weight = out_weight / cell_area

    if field == "counts":
        weight_list: list = out_weight.astype(np.int64).tolist()
    else:
        if decimals is not None:
            out_weight = np.round(out_weight, decimals)
        weight_list = out_weight.tolist()

    return {**bins_payload, field: weight_list}


def histogram_struct_type(
    spec: Optional[HistogramSpec], decimals: Optional[int], source_dtype
) -> pa.StructType:
    """Arrow struct type for a histogram column, matching build_histogram's
    output shape for the given spec/decimals/source dtype."""
    spec = spec or HistogramSpec()
    field = weight_field_name(spec)
    weight_type = pa.int64() if field == "counts" else pa.float64()

    if spec.binned:
        return pa.struct(
            [
                pa.field("left", pa.list_(pa.float64())),
                pa.field("right", pa.list_(pa.float64())),
                pa.field(field, pa.list_(weight_type)),
            ]
        )

    if decimals is not None and decimals <= 0:
        values_type = pa.int64()
    elif decimals is not None:
        values_type = pa.float64()
    else:
        values_type = pa.from_numpy_dtype(source_dtype)
    return pa.struct(
        [
            pa.field("values", pa.list_(values_type)),
            pa.field(field, pa.list_(weight_type)),
        ]
    )


def _weighted_groupby(keys: np.ndarray, w: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """Group `w` by `keys`, returning (sorted unique keys, summed weight per key)."""
    unique_keys, inverse = np.unique(keys, return_inverse=True)
    summed = np.zeros(len(unique_keys), dtype=np.float64)
    np.add.at(summed, inverse, w)
    return unique_keys, summed


def _categorical(
    arr: np.ndarray, w: np.ndarray, decimals: Optional[int]
) -> tuple[list, np.ndarray]:
    if decimals is not None and decimals <= 0:
        keys = np.round(arr, decimals).astype(np.int64)
    elif decimals is not None:
        keys = np.round(arr, decimals)
    else:
        keys = arr
    unique_keys, summed = _weighted_groupby(keys, w)
    return unique_keys.tolist(), summed


def _binned_explicit(
    arr: np.ndarray, w: np.ndarray, edges: Union[tuple, Sequence[float]]
) -> tuple[list, list, np.ndarray]:
    edges_arr = np.asarray(edges, dtype=np.float64)
    # numpy's own bin convention: half-open [a, b) except the last bin,
    # which is closed -- exactly the semantics --hist-bins documents.
    dense_counts, _ = np.histogram(arr, bins=edges_arr, weights=w)
    nonzero = dense_counts != 0
    lefts = edges_arr[:-1][nonzero]
    rights = edges_arr[1:][nonzero]
    return lefts.tolist(), rights.tolist(), dense_counts[nonzero]


def _binned_uniform(
    arr: np.ndarray, w: np.ndarray, width: float, origin: float
) -> tuple[list, list, np.ndarray]:
    bin_idx = np.floor((arr - origin) / width).astype(np.int64)
    unique_bins, summed = _weighted_groupby(bin_idx, w)
    lefts = origin + unique_bins.astype(np.float64) * width
    rights = lefts + width
    return lefts.tolist(), rights.tolist(), summed
