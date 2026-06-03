"""
Raster sampling context for --transfer sample.

_SampleIndexer holds all shared state (open raster, transformer, indexer
config) and exposes process_nn / process_bilinear / process_bicubic /
process_lanczos as bound methods.  Each method accepts only a rasterio
Window and returns None, making them drop-in callables for
ThreadPoolExecutor.map.

Keeping the sampling logic here (rather than as closures inside
initial_index) makes the methods independently unit-testable and keeps
common.py focused on orchestration.
"""

from __future__ import annotations

import dataclasses
from typing import Any, Callable, Optional

import numpy as np
import pandas as pd
import pyarrow as pa
import pyproj
import rasterio as rio
import xarray as xr
from rasterio.warp import transform_bounds

from raster2dggs.indexers.rasterindexer import _is_nan
from raster2dggs.interfaces import IRasterIndexer

_BICUBIC_OFFSETS = np.array([-1, 0, 1, 2], dtype=float)
_BICUBIC_OFFSETS_INT = np.array([-1, 0, 1, 2], dtype=int)


def _bicubic_kernel(t: np.ndarray, a: float = -0.5) -> np.ndarray:
    """Keys cubic convolution kernel.

    a = -0.5 (default, Keys 1989): best approximation to sinc.
    a = -0.75: produces slightly more ringing but commonly used in image
    processing libraries (e.g. OpenCV).

    |t| ≤ 1: (a+2)|t|³ − (a+3)|t|² + 1
    1 < |t| ≤ 2: a|t|³ − 5a|t|² + 8a|t| − 4a
    """
    t = np.abs(t)
    return np.where(
        t <= 1,
        (a + 2) * t**3 - (a + 3) * t**2 + 1,
        np.where(t <= 2, a * t**3 - 5 * a * t**2 + 8 * a * t - 4 * a, 0.0),
    )


def _lanczos_kernel(t: np.ndarray, a: int = 3) -> np.ndarray:
    """Lanczos resampling kernel with `a` lobes (default: 3, giving a 6×6 stencil).

    L(t) = sinc(t) · sinc(t/a)   for |t| < a
    L(t) = 0                      for |t| ≥ a
    where sinc(x) = sin(πx) / (πx), L(0) = 1.
    """
    t = np.abs(t)
    with np.errstate(divide="ignore", invalid="ignore"):
        result = np.where(
            t == 0,
            1.0,
            np.where(
                t < a,
                a * np.sin(np.pi * t) * np.sin(np.pi * t / a) / (np.pi**2 * t**2),
                0.0,
            ),
        )
    return result


@dataclasses.dataclass(repr=False)
class _SampleIndexer:
    """Shared context for all --transfer sample process methods.

    Instantiate once per raster open; pass bound methods (e.g.
    ctx.process_bicubic) to ThreadPoolExecutor.map.
    """

    src: rio.DatasetReader
    da: xr.DataArray
    inverse_transformer: pyproj.Transformer
    nodata: Any
    indexer: IRasterIndexer
    resolution: int
    parent_res: int
    selected_labels: tuple
    nodata_policy: str
    emit_nodata_value: Optional[Any]
    write_result: Callable
    # Kernel tuning parameters — use field(default=…) so the dataclass
    # keeps all non-default fields before these.
    bicubic_a: float = dataclasses.field(default=-0.5)
    lanczos_lobes: int = dataclasses.field(default=3)

    def __post_init__(self):
        lobe = self.lanczos_lobes
        self._lanczos_ns: int = 2 * lobe
        self._lanczos_offsets_int: np.ndarray = np.arange(
            -(lobe - 1), lobe + 1, dtype=int
        )
        self._lanczos_offsets_float: np.ndarray = self._lanczos_offsets_int.astype(
            float
        )

    def _enumerate_cells(self, window):
        """Return (cells, frac_rows, frac_cols) for all DGGS cells whose
        centres fall within the window's geographic bbox.

        frac_rows / frac_cols are fractional pixel-centre coordinates in
        raster space: (0.0, 0.0) is the centre of the top-left pixel.

        Returns (None, None, None) if no cells qualify.
        """
        win_left, win_bottom, win_right, win_top = self.src.window_bounds(window)
        min_lon, min_lat, max_lon, max_lat = transform_bounds(
            self.src.crs, "EPSG:4326", win_left, win_bottom, win_right, win_top
        )
        cells = list(
            self.indexer.cells_in_bbox(
                min_lon, min_lat, max_lon, max_lat, self.resolution
            )
        )
        if not cells:
            return None, None, None

        cell_lons, cell_lats = self.indexer.cells_to_lonlat_arrays(pd.Series(cells))
        # Pass as Python lists to avoid pyproj trying float(array) on
        # 1-element numpy arrays, triggering a NumPy DeprecationWarning.
        _xs, _ys = self.inverse_transformer.transform(
            cell_lons.tolist(), cell_lats.tolist()
        )
        cell_xs = np.asarray(_xs)
        cell_ys = np.asarray(_ys)

        # ~transform maps (x, y) → (col_ul, row_ul) in the UL-corner pixel
        # system; subtract 0.5 to get pixel-centre coords where (0.0, 0.0)
        # is the centre of the top-left pixel.
        inv = ~self.src.transform
        frac_cols = inv.a * cell_xs + inv.b * cell_ys + inv.c - 0.5
        frac_rows = inv.d * cell_xs + inv.e * cell_ys + inv.f - 0.5

        return cells, frac_rows, frac_cols

    def _expand_window(self, window, margin: int):
        """Expand window by margin pixels, clamped to raster bounds."""
        row_off = max(0, window.row_off - margin)
        col_off = max(0, window.col_off - margin)
        row_end = min(self.src.height, window.row_off + window.height + margin)
        col_end = min(self.src.width, window.col_off + window.width + margin)
        return rio.windows.Window(
            col_off, row_off, col_end - col_off, row_end - row_off
        )

    def _build_and_write(self, cells, samples, nd_mask, window):
        """Build the output DataFrame and write to the stage-1 store.

        samples is (n_cells, n_bands) float with NaN where nodata.
        nd_mask is a bool array of length n_cells.
        """
        wide = pd.DataFrame(
            {label: samples[:, i] for i, label in enumerate(self.selected_labels)}
        )
        index_col = self.indexer.index_col(self.resolution)
        partition_col = self.indexer.partition_col(self.parent_res)
        wide[index_col] = cells
        wide[partition_col] = list(
            self.indexer.single_parent_cells(cells, self.parent_res)
        )
        if self.nodata_policy.lower() == "omit":
            wide = wide[~nd_mask].reset_index(drop=True)
        elif self.nodata_policy.lower() == "emit":
            fill = (
                self.emit_nodata_value
                if self.emit_nodata_value is not None
                else self.nodata
            )
            for label in self.selected_labels:
                if pd.isna(fill):
                    wide.loc[nd_mask, label] = np.nan
                else:
                    wide.loc[nd_mask, label] = fill
        if wide.empty:
            return None
        result = pa.Table.from_pandas(wide, preserve_index=False)
        self.write_result(result, window)
        return None

    def process_nn(self, window):
        """Nearest-neighbour sample: assign each DGGS cell the value of the
        closest raster pixel (floor(frac + 0.5) in pixel-centre coordinates).
        """
        cells, frac_rows, frac_cols = self._enumerate_cells(window)
        if cells is None:
            return None

        # Nearest pixel: floor(frac + 0.5) matches rasterio's rowcol default.
        nn_rows = np.floor(frac_rows + 0.5).astype(int)
        nn_cols = np.floor(frac_cols + 0.5).astype(int)
        local_rows = nn_rows - window.row_off
        local_cols = nn_cols - window.col_off
        in_win = (
            (local_rows >= 0)
            & (local_rows < window.height)
            & (local_cols >= 0)
            & (local_cols < window.width)
        )
        if not np.any(in_win):
            return None

        cells = [c for c, m in zip(cells, in_win) if m]
        local_rows = local_rows[in_win]
        local_cols = local_cols[in_win]

        win_data = self.da.rio.isel_window(window).values  # (bands, H, W)
        samples = win_data[:, local_rows, local_cols].T.astype(float)
        if self.nodata is not None and not _is_nan(self.nodata):
            samples[samples == self.nodata] = np.nan
        nd_mask = np.any(np.isnan(samples), axis=1)

        return self._build_and_write(cells, samples, nd_mask, window)

    def process_bilinear(self, window):
        """Bilinear sample: weighted average of the 2×2 pixel neighbourhood
        surrounding each DGGS cell centre.

        Renormalises weights over valid (non-nodata, in-bounds) corners; a
        cell is emitted as nodata only when fewer than 2 of its 4 corners are
        valid.  Window assignment uses the nearest-pixel rule (same as NN) to
        avoid a half-pixel-wide strip of dropped cells at window boundaries.
        """
        cells, frac_rows, frac_cols = self._enumerate_cells(window)
        if cells is None:
            return None

        # Assign cells by nearest pixel — same rule as NN — so the
        # assignment exactly matches what cells_in_bbox discovers and
        # no cells fall through window-boundary gaps.
        # (Using floor-pixel assignment instead causes a half-pixel-
        # wide strip of dropped cells at every window boundary because
        # cells with frac ∈ (row_off+height−0.5, row_off+height) have
        # their WGS84 centre in the next window's bbox but their floor
        # pixel still in this window.)
        nn_rows = np.floor(frac_rows + 0.5).astype(int)
        nn_cols = np.floor(frac_cols + 0.5).astype(int)
        local_nn_rows = nn_rows - window.row_off
        local_nn_cols = nn_cols - window.col_off
        in_win = (
            (local_nn_rows >= 0)
            & (local_nn_rows < window.height)
            & (local_nn_cols >= 0)
            & (local_nn_cols < window.width)
        )
        if not np.any(in_win):
            return None

        cells = [c for c, m in zip(cells, in_win) if m]
        frac_rows = frac_rows[in_win]
        frac_cols = frac_cols[in_win]

        floor_rows = np.floor(frac_rows).astype(int)
        floor_cols = np.floor(frac_cols).astype(int)
        dr = frac_rows - floor_rows  # fractional row offset [0, 1)
        dc = frac_cols - floor_cols  # fractional col offset [0, 1)

        # Read with 1-pixel margin so the bilinear stencil is always
        # available, even when the floor pixel is in the adjacent window
        # (cells with frac ∈ [nn−0.5, nn) have floor = nn−1).
        exp = self._expand_window(window, margin=1)
        win_data = self.da.rio.isel_window(exp).values  # (bands, H, W)
        exp_h, exp_w = win_data.shape[1], win_data.shape[2]

        r0_raw = floor_rows - exp.row_off
        c0_raw = floor_cols - exp.col_off
        r1_raw = r0_raw + 1
        c1_raw = c0_raw + 1

        # Stencil extends outside the raster — emit nodata rather than
        # inventing values by repeating the edge.
        outside_raster = (
            (r0_raw < 0) | (r1_raw >= exp_h) | (c0_raw < 0) | (c1_raw >= exp_w)
        )

        r0 = np.clip(r0_raw, 0, exp_h - 1)
        c0 = np.clip(c0_raw, 0, exp_w - 1)
        r1 = np.clip(r1_raw, 0, exp_h - 1)
        c1 = np.clip(c1_raw, 0, exp_w - 1)

        dr = dr[:, np.newaxis]  # (n_cells, 1) for broadcasting
        dc = dc[:, np.newaxis]

        has_nodata = self.nodata is not None and not _is_nan(self.nodata)

        def _corner(rows, cols):
            s = win_data[:, rows, cols].T.astype(float)  # (n_cells, bands)
            if has_nodata:
                s[s == self.nodata] = np.nan
            return s

        f00 = _corner(r0, c0)
        f01 = _corner(r0, c1)
        f10 = _corner(r1, c0)
        f11 = _corner(r1, c1)

        # Fast path: no OOB and all corners valid.
        if not np.any(outside_raster) and not (
            np.any(np.isnan(f00))
            or np.any(np.isnan(f01))
            or np.any(np.isnan(f10))
            or np.any(np.isnan(f11))
        ):
            samples = (
                (1 - dr) * (1 - dc) * f00
                + (1 - dr) * dc * f01
                + dr * (1 - dc) * f10
                + dr * dc * f11
            )
            return self._build_and_write(
                cells, samples, np.zeros(len(cells), dtype=bool), window
            )

        # Slow path: mark OOB corners as NaN so renormalization handles
        # them uniformly (valid_count < 2 catches cells with no good data).
        f00[outside_raster] = np.nan
        f01[outside_raster] = np.nan
        f10[outside_raster] = np.nan
        f11[outside_raster] = np.nan

        # Renormalise weights over valid (non-NaN) corners; require ≥ 2.
        nan00 = np.isnan(f00)
        nan01 = np.isnan(f01)
        nan10 = np.isnan(f10)
        nan11 = np.isnan(f11)
        valid_count = (
            (~nan00).astype(int)
            + (~nan01).astype(int)
            + (~nan10).astype(int)
            + (~nan11).astype(int)
        )  # (n_cells, bands)
        w00 = np.where(nan00, 0.0, (1 - dr) * (1 - dc))
        w01 = np.where(nan01, 0.0, (1 - dr) * dc)
        w10 = np.where(nan10, 0.0, dr * (1 - dc))
        w11 = np.where(nan11, 0.0, dr * dc)
        f00 = np.where(nan00, 0.0, f00)
        f01 = np.where(nan01, 0.0, f01)
        f10 = np.where(nan10, 0.0, f10)
        f11 = np.where(nan11, 0.0, f11)
        total_w = w00 + w01 + w10 + w11  # (n_cells, bands)
        with np.errstate(invalid="ignore"):  # 0/0 → NaN is intentional
            samples = (w00 * f00 + w01 * f01 + w10 * f10 + w11 * f11) / total_w
        nd_mask = (valid_count < 2).any(axis=1)

        return self._build_and_write(cells, samples, nd_mask, window)

    def process_bicubic(self, window):
        """Bicubic (Keys cubic convolution) sample: 4×4 pixel stencil.

        Uses the Keys 1989 piecewise-cubic kernel with shape parameter
        `self.bicubic_a` (default -0.5, which best approximates a sinc;
        -0.75 matches OpenCV/PIL and produces slightly more ringing).

        Renormalises weights over valid pixels; a cell is emitted as nodata
        when fewer than 4 of its 16 stencil pixels are valid.
        """
        cells, frac_rows, frac_cols = self._enumerate_cells(window)
        if cells is None:
            return None

        nn_rows = np.floor(frac_rows + 0.5).astype(int)
        nn_cols = np.floor(frac_cols + 0.5).astype(int)
        local_nn_rows = nn_rows - window.row_off
        local_nn_cols = nn_cols - window.col_off
        in_win = (
            (local_nn_rows >= 0)
            & (local_nn_rows < window.height)
            & (local_nn_cols >= 0)
            & (local_nn_cols < window.width)
        )
        if not np.any(in_win):
            return None

        cells = [c for c, m in zip(cells, in_win) if m]
        frac_rows = frac_rows[in_win]
        frac_cols = frac_cols[in_win]

        floor_rows = np.floor(frac_rows).astype(int)
        floor_cols = np.floor(frac_cols).astype(int)
        dr = frac_rows - floor_rows
        dc = frac_cols - floor_cols

        exp = self._expand_window(window, margin=2)
        win_data = self.da.rio.isel_window(exp).values  # (bands, H, W)
        exp_h, exp_w = win_data.shape[1], win_data.shape[2]
        n = len(cells)
        bands = win_data.shape[0]

        # 4×4 stencil: offsets [-1, 0, 1, 2] from floor pixel
        local_r = floor_rows[:, np.newaxis] + _BICUBIC_OFFSETS_INT - exp.row_off
        local_c = floor_cols[:, np.newaxis] + _BICUBIC_OFFSETS_INT - exp.col_off
        r_bcast = np.broadcast_to(local_r[:, :, np.newaxis], (n, 4, 4))
        c_bcast = np.broadcast_to(local_c[:, np.newaxis, :], (n, 4, 4))
        oob = (r_bcast < 0) | (r_bcast >= exp_h) | (c_bcast < 0) | (c_bcast >= exp_w)
        r_safe = np.clip(r_bcast, 0, exp_h - 1)
        c_safe = np.clip(c_bcast, 0, exp_w - 1)

        gathered = win_data[:, r_safe.reshape(-1), c_safe.reshape(-1)]
        pixel_values = (
            gathered.reshape(bands, n, 4, 4).transpose(1, 2, 3, 0).astype(float)
        )  # (n, 4, 4, bands)

        if self.nodata is not None and not _is_nan(self.nodata):
            pixel_values[pixel_values == self.nodata] = np.nan

        row_t = np.abs(dr[:, np.newaxis] - _BICUBIC_OFFSETS)  # (n, 4)
        col_t = np.abs(dc[:, np.newaxis] - _BICUBIC_OFFSETS)  # (n, 4)
        row_w = _bicubic_kernel(row_t, a=self.bicubic_a)  # (n, 4)
        col_w = _bicubic_kernel(col_t, a=self.bicubic_a)  # (n, 4)
        weights_2d = row_w[:, :, np.newaxis] * col_w[:, np.newaxis, :]  # (n, 4, 4)

        # Fast path: all stencil pixels in-bounds and non-nodata.
        if not np.any(oob) and not np.any(np.isnan(pixel_values)):
            samples = (weights_2d[:, :, :, np.newaxis] * pixel_values).sum(axis=(1, 2))
            return self._build_and_write(
                cells, samples, np.zeros(n, dtype=bool), window
            )

        # Slow path: renormalise over valid pixels; require ≥ 4 per band.
        pixel_values[oob] = np.nan
        nan_mask = np.isnan(pixel_values)  # (n, 4, 4, bands)
        valid_count = (~nan_mask).sum(axis=(1, 2))  # (n, bands)
        masked_w = np.where(nan_mask, 0.0, weights_2d[:, :, :, np.newaxis])
        pixel_values_safe = np.where(nan_mask, 0.0, pixel_values)
        total_w = masked_w.sum(axis=(1, 2))  # (n, bands)
        with np.errstate(invalid="ignore"):
            samples = (masked_w * pixel_values_safe).sum(axis=(1, 2)) / total_w
        nd_mask = (valid_count < 4).any(axis=1)

        return self._build_and_write(cells, samples, nd_mask, window)

    def process_lanczos(self, window):
        """Lanczos windowed-sinc sample: (2·lobes)×(2·lobes) pixel stencil.

        Uses `self.lanczos_lobes` (default 3, giving a 6×6 stencil) to
        control quality vs. cost.  The kernel is L(t) = sinc(t)·sinc(t/a)
        for |t| < a, zero elsewhere (Lanczos-3 by default).

        Renormalises weights over valid pixels; a cell is emitted as nodata
        when fewer than 4 of its stencil pixels are valid.
        """
        cells, frac_rows, frac_cols = self._enumerate_cells(window)
        if cells is None:
            return None

        nn_rows = np.floor(frac_rows + 0.5).astype(int)
        nn_cols = np.floor(frac_cols + 0.5).astype(int)
        local_nn_rows = nn_rows - window.row_off
        local_nn_cols = nn_cols - window.col_off
        in_win = (
            (local_nn_rows >= 0)
            & (local_nn_rows < window.height)
            & (local_nn_cols >= 0)
            & (local_nn_cols < window.width)
        )
        if not np.any(in_win):
            return None

        cells = [c for c, m in zip(cells, in_win) if m]
        frac_rows = frac_rows[in_win]
        frac_cols = frac_cols[in_win]

        floor_rows = np.floor(frac_rows).astype(int)
        floor_cols = np.floor(frac_cols).astype(int)
        dr = frac_rows - floor_rows
        dc = frac_cols - floor_cols

        lobe = self.lanczos_lobes
        ns = self._lanczos_ns
        offsets_int = self._lanczos_offsets_int
        offsets_float = self._lanczos_offsets_float

        exp = self._expand_window(window, margin=lobe)
        win_data = self.da.rio.isel_window(exp).values  # (bands, H, W)
        exp_h, exp_w = win_data.shape[1], win_data.shape[2]
        n = len(cells)
        bands = win_data.shape[0]

        local_r = floor_rows[:, np.newaxis] + offsets_int - exp.row_off
        local_c = floor_cols[:, np.newaxis] + offsets_int - exp.col_off
        r_bcast = np.broadcast_to(local_r[:, :, np.newaxis], (n, ns, ns))
        c_bcast = np.broadcast_to(local_c[:, np.newaxis, :], (n, ns, ns))
        oob = (r_bcast < 0) | (r_bcast >= exp_h) | (c_bcast < 0) | (c_bcast >= exp_w)
        r_safe = np.clip(r_bcast, 0, exp_h - 1)
        c_safe = np.clip(c_bcast, 0, exp_w - 1)

        gathered = win_data[:, r_safe.reshape(-1), c_safe.reshape(-1)]
        pixel_values = (
            gathered.reshape(bands, n, ns, ns).transpose(1, 2, 3, 0).astype(float)
        )  # (n, ns, ns, bands)

        if self.nodata is not None and not _is_nan(self.nodata):
            pixel_values[pixel_values == self.nodata] = np.nan

        row_t = np.abs(dr[:, np.newaxis] - offsets_float)  # (n, ns)
        col_t = np.abs(dc[:, np.newaxis] - offsets_float)  # (n, ns)
        row_w = _lanczos_kernel(row_t, a=lobe)  # (n, ns)
        col_w = _lanczos_kernel(col_t, a=lobe)  # (n, ns)
        weights_2d = row_w[:, :, np.newaxis] * col_w[:, np.newaxis, :]  # (n, ns, ns)

        # Fast path: all stencil pixels in-bounds and non-nodata.
        if not np.any(oob) and not np.any(np.isnan(pixel_values)):
            samples = (weights_2d[:, :, :, np.newaxis] * pixel_values).sum(axis=(1, 2))
            return self._build_and_write(
                cells, samples, np.zeros(n, dtype=bool), window
            )

        # Slow path: renormalise over valid pixels; require ≥ 4 per band.
        pixel_values[oob] = np.nan
        nan_mask = np.isnan(pixel_values)  # (n, ns, ns, bands)
        valid_count = (~nan_mask).sum(axis=(1, 2))  # (n, bands)
        masked_w = np.where(nan_mask, 0.0, weights_2d[:, :, :, np.newaxis])
        pixel_values_safe = np.where(nan_mask, 0.0, pixel_values)
        total_w = masked_w.sum(axis=(1, 2))  # (n, bands)
        with np.errstate(invalid="ignore"):
            samples = (masked_w * pixel_values_safe).sum(axis=(1, 2)) / total_w
        nd_mask = (valid_count < 4).any(axis=1)

        return self._build_and_write(cells, samples, nd_mask, window)
