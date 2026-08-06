"""
Stage 1 must emit exactly one row per pixel it keeps.

An earlier rewrite of ``index_func`` retained every nodata pixel under the
'omit' policy -- 5x the rows it should have -- and the entire test suite passed,
because the extra rows carried NaN and were dropped again by the Stage 2
aggregation. The only visible symptom was cost. These assertions make the row
count itself the thing under test.

``index_func`` is defined once on the shared base class and the filtering does
not vary by DGGS, so H3 alone covers it; the cross-DGGS parametrization used
elsewhere applies to behaviour that differs per backend.
"""

import numpy as np
import pytest
import xarray as xr

from raster2dggs.indexerfactory import indexer_instance

_RES = 12
_PARENT_RES = 6
# Small enough to enumerate the expected count by hand, over land in WGS84.
_H, _W = 4, 5
_LON0, _LAT0 = 174.0, -41.0
_STEP = 1e-3


def _block(values: np.ndarray) -> xr.DataArray:
    n_bands, height, width = values.shape
    return xr.DataArray(
        values,
        dims=("band", "y", "x"),
        coords={
            "band": np.arange(1, n_bands + 1),
            "y": _LAT0 - np.arange(height) * _STEP,
            "x": _LON0 + np.arange(width) * _STEP,
        },
    )


def _index(values, nodata_policy="omit", **kwargs):
    indexer = indexer_instance("h3")
    return indexer.index_func(
        _block(values),
        _RES,
        _PARENT_RES,
        nodata=np.nan,
        nodata_policy=nodata_policy,
        **kwargs,
    )


def _values(n_bands=3):
    return np.arange(n_bands * _H * _W, dtype=np.float64).reshape(n_bands, _H, _W) + 1.0


def test_omit_drops_pixels_that_are_nodata_in_every_band():
    values = _values()
    # Three pixels nodata in all bands; one nodata in two of three bands, which
    # must survive because it still holds a value somewhere.
    values[:, 0, 0] = np.nan
    values[:, 1, 1] = np.nan
    values[:, 2, 2] = np.nan
    values[0:2, 3, 3] = np.nan

    assert _index(values).num_rows == (_H * _W) - 3


def test_omit_on_an_all_nodata_window_yields_no_rows():
    """The case that regressed: with every band entirely NaN, a row filter
    computed per surviving band has no bands left to consult."""
    values = np.full((3, _H, _W), np.nan)

    assert _index(values).num_rows == 0


def test_omit_keeps_every_pixel_when_there_is_no_nodata():
    assert _index(_values()).num_rows == _H * _W


@pytest.mark.parametrize("fill", [None, -9999.0], ids=["nan-fill", "value-fill"])
def test_emit_keeps_every_pixel_regardless_of_nodata(fill):
    values = _values()
    values[:, 0, 0] = np.nan
    values[:, 1, 1] = np.nan

    table = _index(values, nodata_policy="emit", emit_nodata_value=fill)

    assert table.num_rows == _H * _W


def test_row_count_is_independent_of_band_count():
    """One row per pixel, not per pixel per band. Routing the block through a
    long-form dataframe made this scale with bands."""
    counts = {n: _index(_values(n_bands=n)).num_rows for n in (1, 3, 10)}

    assert set(counts.values()) == {_H * _W}, counts
