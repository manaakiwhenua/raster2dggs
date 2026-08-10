"""
--cell-id: the integer output mode, and the spec/indexer capability agreement.
"""

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from classes.base import clear_folder
from classes.helpers import make_raster
from click.testing import CliRunner
from data.datapaths import TEST_OUTPUT_PATH

from raster2dggs.cli import cli
from raster2dggs.cli_factory import SPECS
from raster2dggs.indexerfactory import indexer_instance

_BOUNDS = (174.0, -41.1, 174.1, -41.0)
_SIZE = 10
_RES = 9


@pytest.mark.parametrize("spec", SPECS, ids=lambda s: s.name)
def test_spec_int_cells_agrees_with_indexer(spec):
    """DGGS_Spec.int_cells drives the CLI choice; CELL_ARROW_TYPE drives the
    pipeline. They describe the same capability and must not drift."""
    indexer = indexer_instance(spec.name)
    assert spec.int_cells == (indexer.CELL_ARROW_TYPE == pa.uint64())


class TestCellIdInt:
    @pytest.fixture(scope="class")
    def raster(self, tmp_path_factory):
        path = tmp_path_factory.mktemp("cellid") / "uniform.tif"
        make_raster(str(path), _BOUNDS, _SIZE, pixel_value=42.0)
        return str(path)

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
                "--processes",
                "1",
                "--overwrite",
                *extra,
            ],
            catch_exceptions=False,
        )
        assert result.exit_code == 0, result.output
        return pq.read_table(TEST_OUTPUT_PATH)

    def test_uint64_mode_writes_uint64_cells(self, raster):
        table = self._invoke(raster, "--cell-id", "uint64")
        assert table.schema.field("h3_09").type == pa.uint64()

    def test_uint64_mode_bijects_to_string_mode(self, raster):
        import h3 as h3py

        text = self._invoke(raster).to_pandas().sort_index()
        ints = self._invoke(raster, "--cell-id", "uint64").to_pandas()
        ints.index = pd.Index(
            [h3py.int_to_str(int(v)) for v in ints.index], name=text.index.name
        )
        ints = ints.sort_index()

        assert list(ints.index) == list(text.index)
        band_cols = [c for c in text.columns if not c.startswith("h3_")]
        for c in band_cols:
            assert (ints[c].values == text[c].values).all()

    def test_string_only_dggs_rejects_uint64_at_parse_time(self, raster):
        result = CliRunner().invoke(
            cli,
            [
                "geohash",
                raster,
                str(TEST_OUTPUT_PATH),
                "-r",
                "5",
                "--cell-id",
                "uint64",
            ],
        )
        assert result.exit_code != 0
        assert "Invalid value for '--cell-id'" in result.output
