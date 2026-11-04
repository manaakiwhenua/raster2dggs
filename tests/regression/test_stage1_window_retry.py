"""
A failed window read of a remote input is retried before it ends the run.

A range read of a remote COG can fail on its own: S3 answering 200 with an
empty body surfaces as RasterioIOError from the window read, and GDAL's HTTP
retry, which covers error codes and dropped connections, never sees it. The
same window reads fine moments later. A window is a function of its bounds and
writes to a fixed file name, so _run_stage1_window re-runs it after a doubling
backoff, a bounded number of times, and names the window when it gives up.

Local inputs get a single attempt: a local read error is not transient, and
waiting on it would only delay the report. Remote-ness is decided once, in the
parent, from the input path or the sources a local VRT lists, and carried to
the workers in their configuration.
"""

import logging

import pytest
from classes.helpers import make_raster
from click.testing import CliRunner
from rasterio.errors import RasterioIOError

from raster2dggs import common
from raster2dggs.cli import cli
from raster2dggs.profiling import PROFILER

_WINDOW = (0, 0, 8, 8)
_BOUNDS = (174.0, -41.2, 174.2, -41.0)


@pytest.fixture
def worker(monkeypatch):
    """A Stage 1 worker whose window function fails a set number of times."""
    sleeps: list[float] = []
    monkeypatch.setattr(common.time, "sleep", sleeps.append)
    monkeypatch.setattr(common, "LOGGER", logging.getLogger("test_window_retry"))

    def configure(failures: int, attempts: int):
        calls: list = []

        def func(window):
            calls.append(window)
            if len(calls) <= failures:
                raise RasterioIOError(
                    "Read failed. See previous exception for details."
                )

        monkeypatch.setitem(common._WORKER, "func", func)
        monkeypatch.setitem(common._WORKER, "read_attempts", attempts)
        return calls, sleeps

    return configure


def test_remote_window_read_is_retried_with_backoff(worker):
    calls, sleeps = worker(failures=2, attempts=4)
    common._run_stage1_window(_WINDOW)
    assert len(calls) == 3
    assert sleeps == [2.0, 4.0]


def test_retries_are_bounded_and_name_the_window(worker, caplog):
    calls, sleeps = worker(failures=10, attempts=4)
    with caplog.at_level(logging.ERROR, logger="test_window_retry"):
        with pytest.raises(RasterioIOError):
            common._run_stage1_window(_WINDOW)
    assert len(calls) == 4
    assert sleeps == [2.0, 4.0, 8.0]
    assert "Window(col_off=0, row_off=0, width=8, height=8)" in caplog.text
    assert "attempt 4 of 4" in caplog.text


def test_retry_wait_is_its_own_profile_phase(worker):
    worker(failures=2, attempts=4)
    PROFILER.reset(enabled=True)
    try:
        common._run_stage1_window(_WINDOW)
        counts = PROFILER.snapshot()["counts"]
    finally:
        PROFILER.reset(enabled=False)
    assert counts["stage1.window_retry"] == 2
    assert counts["stage1.window_total"] == 3


def test_local_input_gets_one_attempt(worker):
    calls, sleeps = worker(failures=1, attempts=1)
    with pytest.raises(RasterioIOError):
        common._run_stage1_window(_WINDOW)
    assert len(calls) == 1
    assert sleeps == []


@pytest.mark.parametrize(
    "path, remote",
    [
        ("/vsicurl/https://example.com/raster.tif", True),
        ("/vsis3/bucket/raster.tif", True),
        ("/vsizip//vsicurl/https://example.com/archive.zip/raster.tif", True),
        ("https://example.com/raster.tif", True),
        ("s3://bucket/raster.tif", True),
        ("/data/raster.tif", False),
        ("raster.tif", False),
        ("C:\\data\\raster.tif", False),
        ("/vsizip//data/archive.zip/raster.tif", False),
    ],
)
def test_is_remote_path(path, remote):
    assert common._is_remote_path(path) is remote


def test_gdal_retry_defaults_yield_to_the_environment(monkeypatch):
    monkeypatch.delenv("GDAL_HTTP_MAX_RETRY", raising=False)
    monkeypatch.delenv("GDAL_HTTP_RETRY_DELAY", raising=False)
    assert common._gdal_http_retry_options(remote=False) == {}
    assert common._gdal_http_retry_options(remote=True) == {
        "GDAL_HTTP_MAX_RETRY": "5",
        "GDAL_HTTP_RETRY_DELAY": "2",
    }
    monkeypatch.setenv("GDAL_HTTP_MAX_RETRY", "9")
    assert common._gdal_http_retry_options(remote=True) == {
        "GDAL_HTTP_RETRY_DELAY": "2"
    }


@pytest.fixture
def local_raster(tmp_path):
    path = tmp_path / "local.tif"
    make_raster(str(path), _BOUNDS, 16, pixel_value=3.0)
    return str(path)


def test_vrt_with_remote_source_counts_as_remote(tmp_path, local_raster):
    remote = "/vsicurl/https://example.invalid/tile.tif"
    vrt = tmp_path / "remote.vrt"
    vrt.write_text(f"""<VRTDataset rasterXSize="16" rasterYSize="16">
  <SRS>EPSG:4326</SRS>
  <GeoTransform>174.0, 0.0125, 0, -41.0, 0, -0.0125</GeoTransform>
  <VRTRasterBand dataType="Float32" band="1">
    <SimpleSource>
      <SourceFilename relativeToVRT="0">{remote}</SourceFilename>
      <SourceBand>1</SourceBand>
      <SourceProperties RasterXSize="16" RasterYSize="16" DataType="Float32"/>
      <SrcRect xOff="0" yOff="0" xSize="16" ySize="16"/>
      <DstRect xOff="0" yOff="0" xSize="16" ySize="16"/>
    </SimpleSource>
  </VRTRasterBand>
</VRTDataset>
""")
    assert common._remote_sources(str(vrt)) == [remote]
    assert common._remote_sources(local_raster) == []


@pytest.fixture
def worker_state(monkeypatch):
    """What each Stage 1 worker is told, and what it makes of it: the
    (remote flag, read attempts, GDAL options) of every worker set up."""
    seen: list[tuple] = []
    original = common._setup_stage1_worker

    def recording(cfg):
        original(cfg)
        seen.append(
            (
                cfg["remote"],
                common._WORKER["read_attempts"],
                dict(common._WORKER["env"].options),
            )
        )

    monkeypatch.setattr(common, "_setup_stage1_worker", recording)
    return seen


def _run(raster, out):
    result = CliRunner().invoke(
        cli,
        ["h3", raster, str(out), "-r", "7", "--processes", "1", "--overwrite"],
        catch_exceptions=False,
    )
    assert result.exit_code == 0, result.output


def test_workers_retry_only_for_remote_inputs(
    monkeypatch, tmp_path, local_raster, worker_state
):
    monkeypatch.delenv("GDAL_HTTP_MAX_RETRY", raising=False)
    monkeypatch.delenv("GDAL_HTTP_RETRY_DELAY", raising=False)

    _run(local_raster, tmp_path / "local")
    assert worker_state[-1] == (False, 1, {})

    # The same local file, treated as if a VRT had listed a remote source.
    monkeypatch.setattr(common, "_remote_sources", lambda _: ["/vsicurl/x.tif"])
    _run(local_raster, tmp_path / "remote")
    assert worker_state[-1] == (
        True,
        common._WINDOW_READ_ATTEMPTS,
        common._GDAL_HTTP_RETRY_DEFAULTS,
    )
