"""
Shared fixtures for the pytest-benchmark suite.

Inputs are synthetic and in-memory so that a benchmark measures the pipeline
phase it names, not GDAL read throughput or filesystem state. Whole-run
timings, including IO, are the job of ``run_baseline.py`` instead.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pyproj
import pytest
import xarray as xr

from raster2dggs.cli_factory import SPECS
from raster2dggs.indexerfactory import INDEXER_LOOKUP

_SPEC_BY_NAME = {s.name: s for s in SPECS}

# Chosen per DGGS to give comparable cell counts over the same extent; they
# are not derivable from the spec. Backends absent here take _DEFAULT_RES.
_RES_BY_DGGS = {
    "geohash": 7,
    "maidenhead": 4,
    "s2": 12,
    "a5": 12,
    "rhp": 8,
    "isea4r": 10,
    "h3": 12,
}
_DEFAULT_RES = 10

# A pixel grid near the test DEM: NZTM metres, and the equivalent degree
# spacing for fixtures that skip reprojection.
_NZTM_ORIGIN = (1_570_000.0, 5_180_000.0)
_PIXEL_M = 10.0
_LONLAT_ORIGIN = (174.0, -41.0)
_DLON = 1.19e-4  # ~10 m at 41 degrees south
_DLAT = 9.04e-5


def one_dggs_per_indexer_module() -> list[str]:
    """One representative per distinct indexer implementation, derived from
    INDEXER_LOOKUP rather than hand-listed. Same rationale as the cross-DGGS
    tests in tests/classes: DGGAL's 16 grids share a single implementation,
    and benchmarking H3 alone hides a per-pixel cost that spans two orders of
    magnitude between backends."""
    seen: set[str] = set()
    names: list[str] = []
    for name, (module_path, _class_name, _extra) in INDEXER_LOOKUP.items():
        if module_path in seen:
            continue
        seen.add(module_path)
        names.append(name)
    return names


DGGS_NAMES = one_dggs_per_indexer_module()


def resolution_for(dggs: str) -> int:
    spec = _SPEC_BY_NAME[dggs]
    return max(spec.min_res, min(_RES_BY_DGGS.get(dggs, _DEFAULT_RES), spec.max_res))


def parent_res_for(dggs: str, resolution: int) -> int:
    """The parent resolution the CLI would default to for this resolution."""
    spec = _SPEC_BY_NAME[dggs]
    return max(spec.min_res, resolution - spec.default_parent_offset)


@pytest.fixture(scope="session")
def transformer():
    """NZTM to WGS84, as the real pipeline builds for the test DEM."""
    return pyproj.Transformer.from_crs("EPSG:2193", "EPSG:4326", always_xy=True)


@pytest.fixture(scope="session")
def make_block():
    """Build a raster block shaped as ``rioxarray`` hands it to ``index_func``.

    ``nodata_fraction`` is applied independently per band, so a pixel is
    dropped by the 'omit' policy only when it is nodata in every band -- which
    is what the real filter does.
    """

    def _make(
        bands: int = 3,
        height: int = 64,
        width: int = 64,
        nodata_fraction: float = 0.2,
        wgs84: bool = False,
    ) -> xr.DataArray:
        rng = np.random.default_rng(seed=(bands, height, width))
        values = rng.random((bands, height, width), dtype=np.float32) * 1000.0
        if nodata_fraction > 0:
            values[rng.random(values.shape) < nodata_fraction] = np.nan
        if wgs84:
            x0, y0 = _LONLAT_ORIGIN
            dx, dy = _DLON, _DLAT
        else:
            x0, y0 = _NZTM_ORIGIN
            dx = dy = _PIXEL_M
        return xr.DataArray(
            values,
            dims=("band", "y", "x"),
            coords={
                "band": np.arange(1, bands + 1),
                # North-up: y descends, as GDAL reports it.
                "y": y0 - np.arange(height) * dy,
                "x": x0 + np.arange(width) * dx,
            },
        )

    return _make


@pytest.fixture(scope="session")
def make_wide():
    """Build the wide frame ``_index_window`` receives: WGS84 x/y columns plus
    one float column per band."""

    def _make(rows: int = 4096, bands: int = 3) -> pd.DataFrame:
        rng = np.random.default_rng(seed=(rows, bands))
        side = int(np.ceil(np.sqrt(rows)))
        lon0, lat0 = _LONLAT_ORIGIN
        grid_lon = np.tile(lon0 + np.arange(side) * _DLON, side)[:rows]
        grid_lat = np.repeat(lat0 - np.arange(side) * _DLAT, side)[:rows]
        data = {"x": grid_lon, "y": grid_lat}
        for band in range(1, bands + 1):
            data[band] = rng.random(rows, dtype=np.float32) * 1000.0
        return pd.DataFrame(data)

    return _make
