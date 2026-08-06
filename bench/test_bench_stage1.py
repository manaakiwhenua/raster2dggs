"""
Stage 1 benchmarks: the per-window path, which is where ``--point`` spends
almost all of its time.

Run with ``pytest bench/`` -- see bench/README.md. These are not collected by
a plain ``pytest`` run, because ``testpaths`` is ``tests``.
"""

from __future__ import annotations

import numpy as np
import pytest
from conftest import DGGS_NAMES, parent_res_for, resolution_for

from raster2dggs.indexerfactory import indexer_instance

_ROWS = 4096  # a 64x64 window's worth of valid pixels
_BANDS = 3


@pytest.mark.parametrize("dggs", DGGS_NAMES)
def test_index_window_leaves_input_alone(make_wide, dggs):
    """The benchmark below reuses one frame across rounds, which is only sound
    because ``_index_window`` returns a new frame rather than modifying the one
    it is given. Asserted rather than assumed: a backend that consumed its
    input would make every round after the first measure a different, smaller
    workload."""
    indexer = indexer_instance(dggs)
    resolution = resolution_for(dggs)
    wide = make_wide(rows=64, bands=_BANDS)
    before = wide.copy(deep=True)
    indexer._index_window(wide, resolution, parent_res_for(dggs, resolution))
    assert list(wide.columns) == list(before.columns)
    assert np.array_equal(wide["x"].to_numpy(), before["x"].to_numpy())
    assert len(wide) == len(before)


@pytest.mark.benchmark(group="dggs cell assignment")
@pytest.mark.parametrize("dggs", DGGS_NAMES)
def test_index_window(benchmark, make_wide, dggs):
    """Cost of turning lon/lat into cell and parent-cell columns, per backend.

    This is the phase reported as ``stage1.dggs_index`` by ``--profile``, and
    the one a batched or native implementation would target. Divide by
    ``_ROWS`` for a per-pixel cost comparable across backends.
    """
    indexer = indexer_instance(dggs)
    resolution = resolution_for(dggs)
    parent_res = parent_res_for(dggs, resolution)
    wide = make_wide(rows=_ROWS, bands=_BANDS)

    result = benchmark(indexer._index_window, wide, resolution, parent_res)

    assert len(result) == _ROWS
    assert indexer.index_col(resolution) in result.columns


@pytest.mark.benchmark(group="stage 1 window, by band count")
@pytest.mark.parametrize("bands", [1, 3, 10])
def test_index_func_bands(benchmark, make_block, transformer, bands):
    """The whole per-window path against band count.

    Cost should scale close to linearly in bands. It scaled worse than that
    while the block was routed through a long-form dataframe and pivoted back
    to wide, since long form carries one row per band per pixel.
    """
    indexer = indexer_instance("h3")
    block = make_block(bands=bands, height=256, width=256)

    table = benchmark(
        indexer.index_func,
        block,
        resolution_for("h3"),
        parent_res_for("h3", resolution_for("h3")),
        nodata=np.nan,
        transformer=transformer,
    )

    assert table.num_rows > 0


@pytest.mark.benchmark(group="stage 1 window, by nodata fraction")
@pytest.mark.parametrize("nodata_fraction", [0.0, 0.5, 0.95])
def test_index_func_nodata(benchmark, make_block, transformer, nodata_fraction):
    """Nodata pixels are dropped before reprojection and indexing, so a sparse
    window should cost proportionately less than a dense one. A flat line here
    means that filter has stopped working."""
    indexer = indexer_instance("h3")
    block = make_block(bands=3, height=256, width=256, nodata_fraction=nodata_fraction)

    benchmark(
        indexer.index_func,
        block,
        resolution_for("h3"),
        parent_res_for("h3", resolution_for("h3")),
        nodata=np.nan,
        transformer=transformer,
    )


@pytest.mark.benchmark(group="stage 1 window, reprojection")
@pytest.mark.parametrize("reproject", [True, False], ids=["nztm-to-wgs84", "no-op"])
def test_index_func_reproject(benchmark, make_block, transformer, reproject):
    """The share of a window attributable to reprojecting pixel centres.

    The gap between these two is what an already-WGS84 source could avoid by
    skipping the transform, which the pipeline currently applies regardless.
    The no-op case gets a WGS84 block, since its pixel centres are used as
    lon/lat directly.
    """
    indexer = indexer_instance("h3")
    block = make_block(bands=3, height=256, width=256, wgs84=not reproject)

    benchmark(
        indexer.index_func,
        block,
        resolution_for("h3"),
        parent_res_for("h3", resolution_for("h3")),
        nodata=np.nan,
        transformer=transformer if reproject else None,
    )
