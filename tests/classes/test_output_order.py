"""
Output rows are sorted by cell ID within every file, and the cell ID column is
delta-encoded, for both ID forms, both write paths, and with compaction.
"""

import pyarrow.parquet as pq
import pytest
from classes.base import clear_folder
from classes.helpers import make_raster
from click.testing import CliRunner
from data.datapaths import TEST_OUTPUT_PATH

from raster2dggs.cli import cli

_BOUNDS = (174.0, -41.1, 174.1, -41.0)
_SIZE = 100  # ~9 pixels per H3 r9 cell, so complete sibling sets exist to compact
_RES = 9
_PARAMS = [
    (cell_id, geo, compact)
    for cell_id in ("string", "uint64")
    for geo in (None, "polygon")
    for compact in (False, True)
]
_IDS = [f"{c}-{g or 'none'}-{'co' if co else 'noco'}" for c, g, co in _PARAMS]


def _files(root):
    return sorted(root.rglob("*.parquet"))


def _column_encodings(path):
    meta = pq.ParquetFile(path).metadata.row_group(0)
    return {
        meta.column(i).path_in_schema: meta.column(i).encodings
        for i in range(meta.num_columns)
    }


@pytest.fixture(scope="module")
def raster(tmp_path_factory):
    path = tmp_path_factory.mktemp("order") / "uniform.tif"
    make_raster(str(path), _BOUNDS, _SIZE, pixel_value=42.0)
    return str(path)


class TestOutputOrder:
    def _invoke(self, raster, *extra):
        if TEST_OUTPUT_PATH.exists():
            clear_folder(TEST_OUTPUT_PATH)
        TEST_OUTPUT_PATH.mkdir(exist_ok=True)
        result = CliRunner().invoke(
            cli,
            [
                "h3",
                raster,
                str(TEST_OUTPUT_PATH),
                "-r",
                str(_RES),
                "-p",
                "1",
                "-o",
                *extra,
            ],
            catch_exceptions=False,
        )
        assert result.exit_code == 0, result.output
        return _files(TEST_OUTPUT_PATH)

    @pytest.mark.parametrize("cell_id,geo,compact", _PARAMS, ids=_IDS)
    def test_rows_sorted_and_cell_column_delta_encoded(
        self, raster, cell_id, geo, compact
    ):
        extra = ["--cell-id", cell_id]
        if geo:
            extra += ["-g", geo]
        if compact:
            extra += ["-co"]
        files = self._invoke(raster, *extra)
        assert files
        for f in files:
            cells = pq.read_table(f, columns=["h3_09"]).column("h3_09").to_pylist()
            assert cells == sorted(cells), f"{f} is not in cell ID order"
            encodings = _column_encodings(f)
            expected = (
                "DELTA_BINARY_PACKED" if cell_id == "uint64" else "DELTA_BYTE_ARRAY"
            )
            assert expected in encodings["h3_09"], encodings
            # the other columns keep the default dictionary encoding
            assert "RLE_DICTIONARY" in encodings["band_1"], encodings

    def test_compaction_still_reduces_rows(self, raster):
        full = sum(pq.ParquetFile(f).metadata.num_rows for f in self._invoke(raster))
        compacted = sum(
            pq.ParquetFile(f).metadata.num_rows for f in self._invoke(raster, "-co")
        )
        assert 0 < compacted < full
