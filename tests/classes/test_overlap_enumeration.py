"""
Cross-DGGS contract for cells_overlapping_bbox and its use by --overlay.

An indexer with SUPPORTS_OVERLAP_ENUMERATION enumerates every cell whose
polygon intersects a bbox (a superset of the centre-based cells_in_bbox), and
overlay must produce the same output through that exact path as through the
padded centre-based fallback.
"""

import importlib

import pandas as pd
import pyarrow.parquet as pq
import pytest
from classes.helpers import make_gradient_raster
from click.testing import CliRunner

from raster2dggs.cli import cli

_BBOX = (174.0, -37.0, 175.0, -36.0)

# (dggs, bbox_res, overlay_res, module_path, class_name)
_CASES = [
    ("h3", 5, 6, "raster2dggs.indexers.h3rasterindexer", "H3RasterIndexer"),
    ("rhp", 6, 6, "raster2dggs.indexers.rhprasterindexer", "RHPRasterIndexer"),
    ("s2", 8, 10, "raster2dggs.indexers.s2rasterindexer", "S2RasterIndexer"),
    ("a5", 8, 11, "raster2dggs.indexers.a5rasterindexer", "A5RasterIndexer"),
]


@pytest.fixture(params=_CASES, ids=[c[0] for c in _CASES])
def case(request):
    dggs, bbox_res, overlay_res, module_path, class_name = request.param
    try:
        mod = importlib.import_module(module_path)
    except ImportError:
        pytest.skip(f"{dggs} extra not installed")
    cls = getattr(mod, class_name)
    return cls(dggs), dggs, bbox_res, overlay_res


def test_flag_is_set(case):
    indexer, *_ = case
    assert indexer.SUPPORTS_OVERLAP_ENUMERATION


def test_overlapping_is_superset_of_centre(case):
    indexer, _, bbox_res, _ = case
    centre = indexer.cells_in_bbox(*_BBOX, bbox_res)
    overlapping = indexer.cells_overlapping_bbox(*_BBOX, bbox_res)
    assert centre and centre <= overlapping
    assert isinstance(overlapping, set)
    assert len(overlapping) > len(centre)  # boundary cells exist for a 1-deg box


def test_overlay_exact_enumeration_matches_padded(case, tmp_path, monkeypatch):
    indexer, dggs, _, overlay_res = case
    raster = tmp_path / "grad.tif"
    make_gradient_raster(str(raster), (174.0, -41.1, 174.1, -41.0), 10)

    def run(out):
        args = [
            dggs,
            str(raster),
            str(out),
            "-r",
            str(overlay_res),
            "--overlay",
            "weighted",
            "-p",
            "1",
            "-o",
        ]
        result = CliRunner().invoke(cli, args, catch_exceptions=False)
        assert result.exit_code == 0, result.output
        return pq.read_table(str(out)).to_pandas().sort_index()

    exact = run(tmp_path / "exact")
    monkeypatch.setattr(type(indexer), "SUPPORTS_OVERLAP_ENUMERATION", False)
    padded = run(tmp_path / "padded")
    pd.testing.assert_frame_equal(exact, padded)
