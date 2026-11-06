"""
--jobs bounds Stage 2 as well as Stage 1.

Stage 2 is a Dask compute on the threaded scheduler, which sizes its pool to
the machine's CPU count unless told otherwise, and each thread holds one
parent cell's working set. address_boundary_issues runs the compute under a
Dask configuration of --jobs threads, or the synchronous scheduler for
one, so the one option bounds concurrency, and memory, in both stages.

The configuration is read where it takes effect: inside _write_output, which
triggers the compute, and at the threaded scheduler's own entry point.
"""

import dask
import dask.base
import dask.threaded
import pytest
from classes.helpers import make_raster
from click.testing import CliRunner

from raster2dggs import common
from raster2dggs.cli import cli

_BOUNDS = (174.0, -41.2, 174.2, -41.0)


@pytest.fixture(scope="module")
def raster(tmp_path_factory):
    path = tmp_path_factory.mktemp("src") / "r.tif"
    make_raster(str(path), _BOUNDS, 48, pixel_value=7.0)
    return str(path)


@pytest.fixture
def observed(monkeypatch):
    """The Dask scheduler configuration in force when Stage 2 computes, and
    the thread count the threaded scheduler is handed, if it runs at all."""
    seen = {}
    original_write = common._write_output

    def write(*args, **kwargs):
        seen["scheduler"] = dask.config.get("scheduler", None)
        seen["num_workers"] = dask.config.get("num_workers", None)
        return original_write(*args, **kwargs)

    original_get = dask.threaded.get

    def get(*args, **kwargs):
        seen["threaded_pool"] = kwargs.get("num_workers") or dask.config.get(
            "num_workers", None
        )
        return original_get(*args, **kwargs)

    monkeypatch.setattr(common, "_write_output", write)
    # Dask resolves a scheduler name through this table, not the module.
    monkeypatch.setitem(dask.base.named_schedulers, "threads", get)
    return seen


def _run(raster, out, processes):
    result = CliRunner().invoke(
        cli,
        [
            "h3",
            raster,
            str(out),
            "-r",
            "9",
            "--jobs",
            str(processes),
            "--overwrite",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0, result.output


def test_stage2_thread_count_is_processes(raster, tmp_path, observed):
    _run(raster, tmp_path / "out", 2)
    assert observed["scheduler"] == "threads"
    assert observed["num_workers"] == 2
    assert observed["threaded_pool"] == 2


def test_one_process_runs_stage2_synchronously(raster, tmp_path, observed):
    _run(raster, tmp_path / "out", 1)
    assert observed["scheduler"] == "synchronous"
    assert "threaded_pool" not in observed


def test_scheduler_config_is_scoped_to_the_run(raster, tmp_path, observed):
    _run(raster, tmp_path / "out", 2)
    assert dask.config.get("num_workers", None) is None
