"""
Stage 2 must see every row for a cell at once.

Stage 1 writes one file per raster window into a directory per parent cell, so a
cell spanning two windows has its pixels in several files. Stage 2 groups by
cell: a cell reaching two partitions is aggregated in each, giving duplicate
rows whose values each cover only part of it.
"""

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from raster2dggs import common
from raster2dggs.indexerfactory import indexer_instance

_RES, _PARENT_RES = 9, 3
_PARENTS = 6
_CELLS_PER_PARENT = 40
_FILES_PER_PARENT = 8


def _index_col():
    return indexer_instance("h3").index_col(_RES)


def _partition_col():
    return indexer_instance("h3").partition_col(_PARENT_RES)


@pytest.fixture
def stage1_store(tmp_path):
    """A store where every cell appears in all of its parent's files, as a cell
    spanning many raster windows would. Returns (path, expected mean per cell)."""
    index_col, partition_col = _index_col(), _partition_col()
    store = tmp_path / "stage1"
    values: dict[str, list[float]] = {}
    for parent in range(_PARENTS):
        # Synthetic uint64 IDs in H3's working form; values need only be
        # distinct and groupable.
        cells = np.array(
            [(0x8000 + parent) * 10**12 + i for i in range(_CELLS_PER_PARENT)],
            dtype=np.uint64,
        )
        for file_no in range(_FILES_PER_PARENT):
            # A distinct value per (cell, file), so a partial mean is detectable.
            vals = np.array(
                [100.0 * file_no + i for i in range(_CELLS_PER_PARENT)],
                dtype=np.float64,
            )
            for cell, v in zip(cells, vals, strict=True):
                values.setdefault(int(cell), []).append(float(v))
            pq.write_to_dataset(
                pa.table(
                    {
                        "band_1": vals,
                        index_col: cells,
                        partition_col: np.full(
                            _CELLS_PER_PARENT, 0x85000 + parent, dtype=np.uint64
                        ),
                    }
                ),
                root_path=str(store),
                partition_cols=[partition_col],
                basename_template=f"{file_no}." + "{i}.parquet",
                existing_data_behavior="overwrite_or_ignore",
            )
    return store, {int(c): float(np.mean(v)) for c, v in values.items()}


def test_one_partition_per_parent(stage1_store):
    """Partition count follows the parent directories, not a size target. A
    size-driven reader puts all six parents in one partition, failing this and
    the next test."""
    store, _ = stage1_store
    ddf = common._read_stage1_by_parent(store, _partition_col(), pa.uint64())

    assert ddf.npartitions == _PARENTS


def test_no_parent_is_split_across_partitions(stage1_store):
    """The invariant itself: each partition holds exactly one whole parent."""
    store, _ = stage1_store
    partition_col = _partition_col()
    ddf = common._read_stage1_by_parent(store, partition_col, pa.uint64())

    per_partition = ddf.map_partitions(
        lambda df: pd.Series([tuple(sorted(set(df[partition_col])))]),
        meta=pd.Series(dtype=object),
    ).compute()

    seen: set[str] = set()
    for parents in per_partition:
        assert len(parents) == 1, f"partition holds several parents: {parents}"
        assert parents[0] not in seen, f"parent split across partitions: {parents[0]}"
        seen.add(parents[0])
    assert len(seen) == _PARENTS


def test_each_cell_is_aggregated_once_over_all_its_files(stage1_store, tmp_path):
    """One row per cell, averaging every file it appears in.

    The property the partitioning protects, and the one that fails quietly. It
    only catches a store large enough to have been split, so the two tests above
    are what pin the mechanism.
    """
    store, expected = stage1_store
    output = tmp_path / "out"
    kwargs = common.assemble_kwargs(
        compression="snappy",
        processes=1,
        aggfuncs=common.create_aggfuncs(("mean",)),
        decimals=None,
        overwrite=True,
        compact=False,
        geo="none",
        point="value",
    )
    kwargs.update(common.resolve_to_internal("value", None, None))

    common.address_boundary_issues(
        indexer_instance("h3"), store, output, _RES, _PARENT_RES, **kwargs
    )

    got = pq.read_table(output).to_pandas()
    assert len(got) == _PARENTS * _CELLS_PER_PARENT
    assert not got.index.duplicated().any(), "a cell was aggregated more than once"
    # Default output is text: H3's boundary conversion renders the synthetic
    # uint64 IDs as hex.
    means = {int(k, 16): v for k, v in got["band_1"].to_dict().items()}
    assert means == pytest.approx(expected)
