import tempfile
from pathlib import Path
from typing import Callable
from unittest import *

import pyarrow.parquet as pq
from click.testing import CliRunner

from data.datapaths import *
from raster2dggs.cli import cli


def read_output(output_dir: Path):
    """Read a Parquet output directory into a PyArrow Table."""
    return pq.read_table(str(output_dir))


def clear_folder(folder):
    for child in folder.iterdir():
        if child.is_dir():
            clear_folder(child)
            child.rmdir()
        else:
            child.unlink()


class TestRunthrough(TestCase):
    """
    Parent class for the smoke tests. Handles temporary output files by
    overriding the built in setup and teardown methods from TestCase.
    """

    def setUp(self):
        TEST_OUTPUT_PATH.mkdir(exist_ok=True)

    def tearDown(self):
        if TEST_OUTPUT_PATH.exists():
            clear_folder(TEST_OUTPUT_PATH)
            TEST_OUTPUT_PATH.rmdir()

    def clearOutFolder(self, folder):
        clear_folder(folder)

    def make_temp_raster(self, make_func: Callable[[str], None]) -> str:
        """Create a temp TIFF, populate it via make_func(path), auto-delete at teardown."""
        tmp = tempfile.NamedTemporaryFile(suffix=".tiff", delete=False)
        make_func(tmp.name)
        self.addCleanup(Path(tmp.name).unlink, missing_ok=True)
        return tmp.name

    def invoke_cli(self, dggs: str, raster, output: Path, resolution, *extra_args):
        """Clear output dir, invoke the CLI, assert exit code 0, return the Result."""
        if output.exists():
            clear_folder(output)
        output.mkdir(exist_ok=True)
        result = CliRunner().invoke(
            cli,
            [dggs, str(raster), str(output), "-r", str(resolution), *extra_args],
            catch_exceptions=False,
        )
        self.assertEqual(result.exit_code, 0, result.output)
        return result
