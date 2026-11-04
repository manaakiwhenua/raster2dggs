"""
With rounding disabled, the written dtype follows what the operation returns.

-d none means "do not round", not "cast back to the source dtype". Stage 1
holds every integer band as float64 so that nodata can be NaN, and the output
schema is what casts a column at write time; a schema that names the source
dtype for a mean, a sum or a bilinear sample either fails the write (a
non-integral or out-of-range value cannot be cast) or, inside a struct,
truncates silently. So the schema names the source dtype only for an operation
that returns one of its inputs, int64 for a count or an integer-valued
operation on an integer source, and float64 for anything derived from an
integer source. A float source keeps its own precision for every operation but
a count.
"""

import numpy as np
import pyarrow as pa
import pytest
import rasterio
from click.testing import CliRunner
from rasterio.transform import from_origin

from raster2dggs.cli import cli

_RES = 10  # h3: ~40 pixels of 10 m per cell, so every aggregation has work


def _write(path, dtype, nodata):
    rng = np.random.default_rng(0)
    if np.issubdtype(np.dtype(dtype), np.integer):
        data = rng.integers(-127, 128, size=(2, 32, 32)).astype(dtype)
    else:
        data = rng.uniform(-127, 128, size=(2, 32, 32)).astype(dtype)
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        width=32,
        height=32,
        count=2,
        dtype=dtype,
        crs="EPSG:2193",
        transform=from_origin(1570000, 5180000, 10, 10),
        nodata=nodata,
    ) as dst:
        dst.write(data)
    return str(path)


@pytest.fixture(scope="module")
def int8_raster(tmp_path_factory):
    return _write(tmp_path_factory.mktemp("src") / "int8.tif", "int8", -128)


@pytest.fixture(scope="module")
def float32_raster(tmp_path_factory):
    return _write(tmp_path_factory.mktemp("src") / "f32.tif", "float32", -9999.0)


def _run(raster, out, *args):
    result = CliRunner().invoke(
        cli,
        ["h3", raster, str(out), "-r", str(_RES), "-d", "none", "-p", "1", *args],
        catch_exceptions=False,
    )
    assert result.exit_code == 0, result.output
    import pyarrow.parquet as pq

    return pq.read_table(str(out))


_INT8_CASES = [
    (("-a", "mean"), pa.float64()),
    (("-a", "median"), pa.float64()),
    (("-a", "std"), pa.float64()),
    (("-a", "var"), pa.float64()),
    (("-a", "max"), pa.int8()),
    (("-a", "min"), pa.int8()),
    (("-a", "mode"), pa.int8()),
    (("-a", "majority"), pa.int8()),
    (("-a", "sum"), pa.int64()),
    (("-a", "range"), pa.int64()),
    (("-a", "count"), pa.int64()),
    (("-a", "nunique"), pa.int64()),
    (("--overlay", "weighted"), pa.float64()),
    (("--overlay", "mass-preserve"), pa.float64()),
    (("--overlay", "density-preserve"), pa.float64()),
    (("--overlay", "mode"), pa.int8()),
    (("--sample", "nn"), pa.int8()),
    (("--sample", "bilinear"), pa.float64()),
]


@pytest.mark.parametrize(
    "args, expected", _INT8_CASES, ids=[" ".join(a) for a, _ in _INT8_CASES]
)
def test_int8_source(int8_raster, tmp_path, args, expected):
    table = _run(int8_raster, tmp_path / "out", *args)
    assert table.schema.field("band_1").type == expected
    assert table.schema.field("band_2").type == expected


def test_multi_agg_struct_members_follow_their_own_aggregation(int8_raster, tmp_path):
    table = _run(int8_raster, tmp_path / "out", "-a", "mean,max,count")
    assert table.schema.field("band_1").type == pa.struct(
        [("mean", pa.float64()), ("max", pa.int8()), ("count", pa.int64())]
    )
    means = [v["mean"] for v in table.column("band_1").to_pylist()]
    assert any(m != round(m) for m in means), "means were truncated to integers"


_FLOAT32_CASES = [
    (("-a", "mean"), pa.float32()),
    (("-a", "sum"), pa.float32()),
    (("-a", "max"), pa.float32()),
    (("-a", "count"), pa.int64()),
    (("--sample", "bilinear"), pa.float32()),
]


@pytest.mark.parametrize(
    "args, expected", _FLOAT32_CASES, ids=[" ".join(a) for a, _ in _FLOAT32_CASES]
)
def test_float32_source_keeps_its_precision(float32_raster, tmp_path, args, expected):
    table = _run(float32_raster, tmp_path / "out", *args)
    assert table.schema.field("band_1").type == expected
