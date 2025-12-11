import sys
import time
from threading import Event, Thread

from classes.base import TestRunthrough
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
            # Non-interactive: just print once
            print(self.label)
            return
        start = time.perf_counter()
        while not self._stop.is_set():
            elapsed = time.perf_counter() - start
            sys.stdout.write(f"\r{self.label} ... {elapsed:6.2f}s")
            sys.stdout.flush()
            time.sleep(self.interval)
        elapsed = time.perf_counter() - start
        sys.stdout.write(f"\r{self.label} ... {elapsed:6.2f}s\n")
        sys.stdout.flush()

    def start(self):
        self._t.start()

    def stop(self):
        self._stop.set()
        self._t.join()


class TestAllDGGS(TestRunthrough):
    def test_all_commands_point_polygon_and_none(self):
        runner = CliRunner()
        for spec in SPECS:
            res = safe_resolution(spec)
            for geo in (None, "point", "polygon"):
                timer = LiveTimer(f"Testing {spec.pretty} --geo {geo}")
                timer.start()
                try:
                    with self.subTest(dggs=spec.name, geo=geo):
                        args = [
                            spec.name,
                            TEST_FILE_PATH,
                            str(TEST_OUTPUT_PATH),
                            "-r",
                            str(res),
                        ]
                        if geo:
                            args += ["-g", geo]
                        result = runner.invoke(cli, args, catch_exceptions=False)
                        self.assertEqual(
                            result.exit_code,
                            0,
                            f"{spec.name} {geo or 'none'} failed:\n{result.output}",
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
