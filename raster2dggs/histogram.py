"""
Shared histogram building for --point histogram and --overlay histogram.

Both code paths collect per-cell contributing pixel values (optionally with
per-pixel weights, e.g. overlap area) and need identical binning/weighting/
normalization semantics; this module is the single implementation both call
into, so a change here (or a bug fix) applies uniformly to both.
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


def build_histogram(
    values: Sequence[float],
    weights: Optional[Sequence[float]] = None,
    spec: Optional[HistogramSpec] = None,
    decimals: Optional[int] = None,
    cell_area: Optional[float] = None,
) -> Optional[dict]:
    """
    Build a {"values": [...], "counts": [...]} histogram from contributing
    pixel values (and optional per-pixel weights).

    - spec is None or categorical (`not spec.binned`): an exact-value
      histogram -- unique (optionally decimals-rounded) values, each paired
      with its summed weight (1 per pixel when weights is None).
    - spec.binned: bins values into `spec.edges` (explicit, numpy
      half-open-except-last-bin-closed convention, out-of-range values
      dropped) or uniform `spec.width`/`spec.origin` bins (unbounded --
      every finite value falls in some bin). Result "values" are bin lower
      edges, float64, sparse (only non-empty bins are reported).

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

    if not spec.binned:
        out_values, out_counts = _categorical(arr, w, decimals)
    elif spec.edges is not None:
        out_values, out_counts = _binned_explicit(arr, w, spec.edges)
    else:
        out_values, out_counts = _binned_uniform(arr, w, spec.width, spec.origin)

    if len(out_values) == 0:
        return None

    if spec.normalize == const.HistNormalize.VALID_OVERLAP:
        if total_valid_weight <= 0:
            return None
        out_counts = out_counts / total_valid_weight
    elif spec.normalize == const.HistNormalize.CELL_AREA:
        if not cell_area or cell_area <= 0:
            return None
        out_counts = out_counts / cell_area

    is_plain_count = (
        spec.weight == const.HistWeight.COUNT
        and spec.normalize == const.HistNormalize.NONE
    )
    if is_plain_count:
        out_counts_list: list = out_counts.astype(np.int64).tolist()
    else:
        if decimals is not None:
            out_counts = np.round(out_counts, decimals)
        out_counts_list = out_counts.tolist()

    return {"values": out_values, "counts": out_counts_list}


def hist_arrow_types(
    spec: Optional[HistogramSpec], decimals: Optional[int], source_dtype
) -> tuple[pa.DataType, pa.DataType]:
    """Arrow (values_type, counts_type) for a histogram struct column, matching
    build_histogram's output shape for the given spec/decimals/source dtype."""
    spec = spec or HistogramSpec()
    if spec.binned:
        values_type = pa.float64()
    elif decimals is not None and decimals <= 0:
        values_type = pa.int64()
    elif decimals is not None:
        values_type = pa.float64()
    else:
        values_type = pa.from_numpy_dtype(source_dtype)
    is_plain_count = (
        spec.weight == const.HistWeight.COUNT
        and spec.normalize == const.HistNormalize.NONE
    )
    counts_type = pa.int64() if is_plain_count else pa.float64()
    return values_type, counts_type


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
) -> tuple[list, np.ndarray]:
    edges_arr = np.asarray(edges, dtype=np.float64)
    # numpy's own bin convention: half-open [a, b) except the last bin,
    # which is closed -- exactly the semantics --hist-bins documents.
    dense_counts, _ = np.histogram(arr, bins=edges_arr, weights=w)
    nonzero = dense_counts != 0
    lower_edges = edges_arr[:-1][nonzero]
    return lower_edges.tolist(), dense_counts[nonzero]


def _binned_uniform(
    arr: np.ndarray, w: np.ndarray, width: float, origin: float
) -> tuple[list, np.ndarray]:
    bin_idx = np.floor((arr - origin) / width).astype(np.int64)
    unique_bins, summed = _weighted_groupby(bin_idx, w)
    lower_edges = origin + unique_bins.astype(np.float64) * width
    return lower_edges.tolist(), summed
