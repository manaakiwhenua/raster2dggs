"""
Exactness of uint64 cell IDs through the pipeline's pandas machinery.

pandas promotes uint64 to float64 wherever it meets int64 or a float column --
a row extracted from a mixed frame, a Python-int index label, int64/uint64
concatenation -- and float64 has 128-step spacing at H3-cell magnitude, so the
promotion silently corrupts cell IDs into other (usually invalid) cells.
"""

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import s2sphere

from raster2dggs import common
from raster2dggs.indexerfactory import indexer_instance


def test_compaction_preserves_uint64_cell_ids_exactly():
    """Compacted parents and their partition values must round-trip exactly.

    The compacted row was extracted with ``group.iloc[0]``, which promotes the
    whole row -- uint64 cells included -- to float64 alongside the float bands;
    the parent label then re-inferred the index as int64, and int64 concatenated
    with uint64 promotes to float64. Either alone corrupts the IDs.
    """
    import h3.api.numpy_int as h3i

    h3x = indexer_instance("h3")
    parent10 = int(h3i.latlng_to_cell(-41.0, 174.0, 10))
    children = [int(c) for c in h3i.cell_to_children(parent10, 11)]
    straggler = int(h3i.latlng_to_cell(-41.2, 174.2, 11))
    p5 = np.uint64(h3i.cell_to_parent(parent10, 5))
    df = pd.DataFrame(
        {"h3_05": p5, "band_1": [7.0] * len(children) + [1.0]},
        index=pd.Index(np.array(children + [straggler], dtype=np.uint64), name="h3_11"),
    )

    out = h3x.compaction(df, 11, 5)

    got = {int(v) for v in out.index}
    assert len(out) == 2
    assert parent10 in got, f"compacted parent corrupted: {[hex(v) for v in got]}"
    assert straggler in got
    assert {int(v) for v in out["h3_05"]} == {int(p5)}
    assert out.index.dtype == np.uint64
    assert out["h3_05"].dtype == np.uint64


def test_s2_face5_parent_survives_the_store_round_trip(tmp_path):
    """S2 cells on faces 4 and 5 exceed int64. The per-parent read must filter
    with a typed scalar: a bare Python int past 2^63 overflows pyarrow's
    default int64 conversion."""
    face5 = s2sphere.CellId.from_face_pos_level(5, 0, 4).id()
    assert face5 > 2**63
    partition_col = "s2_04"
    pq.write_to_dataset(
        pa.table(
            {
                "band_1": [1.0, 2.0, 3.0],
                "s2_12": pa.array([10, 11, 12], type=pa.uint64()),
                partition_col: pa.array([face5, face5, 42], type=pa.uint64()),
            }
        ),
        root_path=str(tmp_path),
        partition_cols=[partition_col],
        existing_data_behavior="overwrite_or_ignore",
    )

    ddf = common._read_stage1_by_parent(tmp_path, partition_col, pa.uint64())

    assert ddf.npartitions == 2
    got = ddf.compute()
    assert set(int(v) for v in got[partition_col]) == {face5, 42}


def test_cell_columns_never_null():
    """uint64 columns quietly become float64 (corrupting IDs above 2^53) the
    moment a null enters them, so Stage 1 must never emit one."""
    import xarray as xr

    h3x = indexer_instance("h3")
    values = np.array([[[1.0, np.nan], [np.nan, 4.0]]])
    block = xr.DataArray(
        values,
        dims=("band", "y", "x"),
        coords={"band": [1], "y": [-41.0, -41.001], "x": [174.0, 174.001]},
    )

    table = h3x.index_func(block, 11, 5, nodata=np.nan)

    for name in ("h3_11", "h3_05"):
        col = table[name]
        assert col.null_count == 0
        assert col.type == pa.uint64()


@pytest.mark.parametrize(
    "dggs,res", [("h3", 8), ("s2", 12), ("a5", 12), ("isea4r", 12)]
)
def test_string_round_trip_is_exact(dggs, res):
    """cells_to_string must be a faithful rendering of the working IDs."""
    indexer = indexer_instance(dggs)
    cells = sorted(indexer.cells_in_bbox(174.0, -41.05, 174.05, -41.0, res))
    assert cells, "fixture bbox produced no cells"
    strings = indexer.cells_to_string(cells)
    assert len(set(strings)) == len(cells), "string forms collide"
    assert all(isinstance(t, str) for t in strings)
