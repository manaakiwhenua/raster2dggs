"""
Cross-DGGS tests for --overlay.

test_output_schema.py's TestOverlay only ever instantiates H3 -- the same
gap that let real bugs in rHEALPix/A5's cells_in_bbox (missing
cos(latitude) correction) and DGGAL's cell_area_m2 (swapped GeoPoint args)
ship unnoticed. --overlay's arithmetic depends on both, so it's worth
checking directly rather than assuming H3 coverage transfers.

Invariants are resolution-agnostic (true regardless of how many cells the
small test raster spans), since exact per-DGGS coarse-resolution tuning
(as test_output_schema.py's _COARSE_RES does for H3 specifically) isn't
needed for them to hold: the source raster is uniform-valued, so an
area-weighted mean of any subset of its pixels equals that value, and a
mass-preserving sum over all output cells equals pixel_value * pixel_count
regardless of how the cells are split.
"""

import pytest
import rasterio
from click.testing import CliRunner

from classes.base import clear_folder
from classes.helpers import make_raster
from data.datapaths import TEST_OUTPUT_PATH
from raster2dggs.cli import cli
from raster2dggs.cli_factory import SPECS
from raster2dggs.indexerfactory import INDEXER_LOOKUP

_BOUNDS = (174.0, -41.1, 174.1, -41.0)
_SIZE = 10
_PIXEL_VALUE = 42.0

_SPEC_BY_NAME = {s.name: s for s in SPECS}
_RES_BY_DGGS = {"maidenhead": 3}
_DEFAULT_RES = 6


def _one_dggs_per_indexer_module():
    """Same rationale as test_resolution_modes.py's TestCellAreaM2: one
    representative per distinct indexer module, derived from
    INDEXER_LOOKUP rather than hand-listed."""
    seen_modules = set()
    names = []
    for name, (module_path, _class_name, _extra) in INDEXER_LOOKUP.items():
        if module_path in seen_modules:
            continue
        seen_modules.add(module_path)
        names.append(name)
    return names


_DGGS_NAMES = _one_dggs_per_indexer_module()


def _safe_res(name):
    spec = _SPEC_BY_NAME[name]
    return max(spec.min_res, min(_RES_BY_DGGS.get(name, _DEFAULT_RES), spec.max_res))


@pytest.fixture
def uniform_raster(tmp_path):
    path = tmp_path / "uniform.tif"
    make_raster(str(path), _BOUNDS, _SIZE, pixel_value=_PIXEL_VALUE)
    return str(path)


@pytest.fixture(params=_DGGS_NAMES)
def dggs_name(request):
    return request.param


def _run_overlay(dggs, raster_path, *overlay_flags, extra_args=()):
    if TEST_OUTPUT_PATH.exists():
        clear_folder(TEST_OUTPUT_PATH)
    TEST_OUTPUT_PATH.mkdir(exist_ok=True)
    result = CliRunner().invoke(
        cli,
        [
            dggs,
            raster_path,
            str(TEST_OUTPUT_PATH),
            "-r",
            str(_safe_res(dggs)),
            "--overlay",
            *overlay_flags,
            *extra_args,
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0, result.output
    import pyarrow.parquet as pq

    return pq.read_table(str(TEST_OUTPUT_PATH))


class TestOverlayCrossDGGS:
    def test_overlay_weighted_matches_uniform_pixel_value(
        self, dggs_name, uniform_raster
    ):
        try:
            table = _run_overlay(dggs_name, uniform_raster, "weighted")
        except ImportError:
            pytest.skip(f"{dggs_name} extra not installed")
        df = table.to_pandas()
        assert not df.empty, f"{dggs_name}: --overlay weighted produced no rows"
        vals = df["band_1"].dropna()
        assert (
            vals - _PIXEL_VALUE
        ).abs().max() < 1e-2, (
            f"{dggs_name}: expected ~{_PIXEL_VALUE}, got {vals.unique()}"
        )

    def test_overlay_mass_preserve_conserves_total(self, dggs_name, uniform_raster):
        try:
            table = _run_overlay(
                dggs_name, uniform_raster, "mass-preserve", extra_args=("-d", "none")
            )
        except ImportError:
            pytest.skip(f"{dggs_name} extra not installed")
        cell_total = table.to_pandas()["band_1"].sum()
        with rasterio.open(uniform_raster) as src:
            raster_total = _PIXEL_VALUE * src.width * src.height
        assert abs(cell_total - raster_total) < raster_total * 1e-3, (
            f"{dggs_name}: mass_preserve total {cell_total:.6f} differs from "
            f"raster total {raster_total:.6f}"
        )
