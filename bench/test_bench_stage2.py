"""
Stage 2 benchmarks: aggregating pixel rows into one row per cell.

Stage 2's share of a run varies enormously with the input -- 4% of a mostly
nodata DEM, 31% of a dense 3-band raster with none -- because what it costs
depends on how many pixels land in each cell, not on how large the raster is.
These benchmarks parametrize that ratio directly rather than leaving it to
whichever fixture a raster happens to be.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
from conftest import parent_res_for, resolution_for

from raster2dggs.indexerfactory import indexer_instance

_ROWS = 200_000


def _indexed_frame(pixels_per_cell: int, bands: int = 3) -> pd.DataFrame:
    """Rows as Stage 2 reads them back: a cell column, a parent column, and
    one value column per band. Cell IDs are synthetic strings of H3's length,
    since the aggregation only ever compares and groups them."""
    rng = np.random.default_rng(seed=(pixels_per_cell, bands))
    n_cells = max(1, _ROWS // pixels_per_cell)
    n_parents = max(1, n_cells // 700)
    cells = np.array([f"8cbb{i:011x}" for i in range(n_cells)], dtype=object)
    parents = np.array([f"86bb{i:011x}" for i in range(n_parents)], dtype=object)
    cell_of_row = rng.integers(0, n_cells, _ROWS)
    data = {
        "cell": cells[cell_of_row],
        "parent": parents[cell_of_row % n_parents],
    }
    for band in range(1, bands + 1):
        data[f"band_{band}"] = rng.random(_ROWS, dtype=np.float32) * 1000.0
    return pd.DataFrame(data)


def _renamed(df: pd.DataFrame, resolution: int, parent_res: int) -> pd.DataFrame:
    indexer = indexer_instance("h3")
    return df.rename(
        columns={
            "cell": indexer.index_col(resolution),
            "parent": indexer.partition_col(parent_res),
        }
    )


@pytest.mark.benchmark(group="stage 2 groupby, by pixels per cell")
@pytest.mark.parametrize("pixels_per_cell", [1, 8, 100])
def test_parent_groupby(benchmark, pixels_per_cell):
    """One aggregation over 200k rows, against how many rows share a cell.

    Few pixels per cell is the expensive end: nearly every row is its own
    group, so the groupby does the most work and compacts the least.
    """
    indexer = indexer_instance("h3")
    resolution = resolution_for("h3")
    parent_res = parent_res_for("h3", resolution)
    df = _renamed(_indexed_frame(pixels_per_cell), resolution, parent_res)

    result = benchmark(
        indexer.parent_groupby, df, resolution, parent_res, [("mean", "mean")], None
    )

    assert len(result) == df[indexer.index_col(resolution)].nunique()


@pytest.mark.benchmark(group="stage 2 groupby, by aggregation count")
@pytest.mark.parametrize("n_aggs", [1, 3], ids=["single", "multi"])
def test_parent_groupby_multi_agg(benchmark, n_aggs):
    """Multiple aggregations produce a struct column per band, which is a
    different code path from the single-aggregation scalar one."""
    indexer = indexer_instance("h3")
    resolution = resolution_for("h3")
    parent_res = parent_res_for("h3", resolution)
    df = _renamed(_indexed_frame(pixels_per_cell=8), resolution, parent_res)
    aggfuncs = [("mean", "mean"), ("max", "max"), ("min", "min")][:n_aggs]

    benchmark(indexer.parent_groupby, df, resolution, parent_res, aggfuncs, None)


@pytest.mark.benchmark(group="stage 2 groupby, decimal rounding")
@pytest.mark.parametrize("decimals", [None, 2], ids=["unrounded", "2dp"])
def test_parent_groupby_decimals(benchmark, decimals):
    """Rounding promotes float32 results to float64 before rounding them, so
    it is not free."""
    indexer = indexer_instance("h3")
    resolution = resolution_for("h3")
    parent_res = parent_res_for("h3", resolution)
    df = _renamed(_indexed_frame(pixels_per_cell=8), resolution, parent_res)

    benchmark(
        indexer.parent_groupby,
        df,
        resolution,
        parent_res,
        [("mean", "mean")],
        decimals,
    )
