import sys
import time
from threading import Event, Thread

import pytest
from classes.base import clear_folder
from data.datapaths import *
from click.testing import CliRunner

from raster2dggs.cli import cli
from raster2dggs.cli_factory import SPECS

RES_BY_DGGS = {
    "maidenhead": 3,
}
DEFAULT_RES = 6


def safe_resolution(spec):
    # Pick a valid resolution within spec bounds (keeps 6 where possible)
    desired = RES_BY_DGGS.get(spec.name, DEFAULT_RES)
    return max(spec.min_res, min(desired, spec.max_res))


class LiveTimer:
    def __init__(self, label: str, interval: float = 0.1):
        self.label = label
        self.interval = interval
        self._stop = Event()
        self._t = Thread(target=self._run, daemon=True)

    def _run(self):
        if not sys.stdout.isatty():
            print(self.label)
            return
        start = time.perf_counter()
        while not self._stop.is_set():
            elapsed = time.perf_counter() - start
            sys.stdout.write(f"\r{self.label} \t{elapsed:6.2f}s")
            sys.stdout.flush()
            time.sleep(self.interval)
        elapsed = time.perf_counter() - start
        sys.stdout.write(f"\r{self.label} \t{elapsed:6.2f}s\n")
        sys.stdout.flush()

    def start(self):
        self._t.start()

    def stop(self):
        self._stop.set()
        self._t.join()


_PARAMS = [
    (spec, geo, compaction)
    for spec in SPECS
    for geo in (None, "point", "polygon")
    for compaction in (True, False)
]

_IDS = [
    f"{spec.name}-{geo or 'none'}-{'co' if compaction else 'noco'}"
    for spec, geo, compaction in _PARAMS
]


class TestAllDGGS:
    def setup_method(self):
        TEST_OUTPUT_PATH.mkdir(exist_ok=True)

    def teardown_method(self):
        if TEST_OUTPUT_PATH.exists():
            clear_folder(TEST_OUTPUT_PATH)
            TEST_OUTPUT_PATH.rmdir()

    @pytest.mark.parametrize("spec,geo,compaction", _PARAMS, ids=_IDS)
    def test_command(self, spec, geo, compaction):
        runner = CliRunner()
        res = safe_resolution(spec)
        timer = LiveTimer(
            f"Testing {spec.pretty}\t--geo {geo or 'none'} {'-co' if compaction else ''}"
        )
        timer.start()
        try:
            args = [
                spec.name,
                TEST_FILE_PATH,
                str(TEST_OUTPUT_PATH),
                "-r",
                str(res),
            ]
            if geo:
                args += ["-g", geo]
            if compaction:
                args += ["-co"]
            result = runner.invoke(cli, args, catch_exceptions=False)
            assert result.exit_code == 0, (
                f"{spec.name} {geo or 'none'} failed:\n{result.output}"
            )
        finally:
            timer.stop()

    def test_help_shows_input_output(self):
        runner = CliRunner()
        res = runner.invoke(cli, "--help")
        assert res.exit_code == 0
        assert "Usage:" in res.output
        assert "Options:" in res.output
        assert "Commands:" in res.output
        for spec in SPECS:
            res = runner.invoke(cli, [spec.name, "--help"], catch_exceptions=False)
            assert res.exit_code == 0
            assert "RASTER_INPUT" in res.output
            assert "OUTPUT_DIRECTORY" in res.output
