"""
Tests for the --profile phase timing facility (raster2dggs/profiling.py).

Covers the three properties that matter: the accumulator is correct when
Stage 1's worker threads hit it concurrently, profiling disabled is a genuine
no-op, and the CLI flag produces a report without altering output.
"""

import re
import threading
import time

import pytest
from classes.base import TestRunthrough, read_output
from classes.helpers import make_raster
from click.testing import CliRunner
from data.datapaths import TEST_OUTPUT_PATH

from raster2dggs.cli import cli
from raster2dggs.profiling import _THREAD_CPU, PROFILER, Profiler

_BOUNDS = (174.0, -41.1, 174.1, -41.0)
_SIZE = 10
_RES = 8

needs_thread_cpu = pytest.mark.skipif(
    _THREAD_CPU is None, reason="platform has no per-thread CPU clock"
)


def _make_raster(path: str) -> None:
    make_raster(path, _BOUNDS, _SIZE, pixel_value=1.0)


def _stat(report: str, label: str) -> float:
    """Pull the number out of a 'Stage 1 <label>: <n>x/%' summary line."""
    line = next(ln for ln in report.splitlines() if label in ln)
    return float(re.search(r"([\d.]+)[x%]", line).group(1))


def _burn_cpu(seconds: float) -> None:
    end = time.perf_counter() + seconds
    while time.perf_counter() < end:
        pass


class TestProfilerUnit:
    """Direct tests of the accumulator, independent of the pipeline."""

    def test_disabled_is_a_noop(self):
        p = Profiler()
        p.reset(enabled=False)
        with p.phase("anything"):
            pass
        p.note("ignored", 1)
        p.stop()
        # Nothing recorded, and the report says so rather than fabricating rows.
        assert "nothing recorded" in p.report()

    def test_records_time_and_counts_when_enabled(self):
        p = Profiler()
        p.reset(enabled=True)
        for _ in range(3):
            with p.phase("work"):
                time.sleep(0.01)
        p.stop()
        report = p.report()
        assert "work" in report
        assert "3" in report

    def test_concurrent_accumulation_is_not_lossy(self):
        """Stage 1 calls phase() from many threads at once; every call must be
        counted.

        This passes with or without the lock on a GIL build, so it is not
        evidence that the lock works — it covers free-threaded builds and any
        future accumulator that could drop updates.
        """
        p = Profiler()
        p.reset(enabled=True)
        n_threads, per_thread = 8, 200

        def worker():
            for _ in range(per_thread):
                with p.phase("concurrent"):
                    pass

        threads = [threading.Thread(target=worker) for _ in range(n_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        p.stop()

        with p._lock:
            assert p._counts["concurrent"] == n_threads * per_thread
            assert p._totals["concurrent"] >= 0.0

    def test_report_is_safe_while_phases_are_recorded(self):
        """report() snapshots under the lock rather than iterating the live
        dicts, so it can be called while workers are still recording."""
        p = Profiler()
        p.reset(enabled=True)

        def writer(worker_id):
            for i in range(50):
                with p.phase(f"phase_{worker_id}_{i}"):
                    pass

        threads = [threading.Thread(target=writer, args=(w,)) for w in range(4)]
        for t in threads:
            t.start()
        try:
            while any(t.is_alive() for t in threads):
                p.report()
        finally:
            for t in threads:
                t.join()
        assert "phase_0_0" in p.report()

    def test_exception_inside_phase_still_records(self):
        """A run that dies partway through is when timings matter most."""
        p = Profiler()
        p.reset(enabled=True)
        try:
            with p.phase("boom"):
                raise ValueError("expected")
        except ValueError:
            pass
        p.stop()
        with p._lock:
            assert p._counts["boom"] == 1

    def test_reset_clears_previous_run(self):
        """The module-level PROFILER is a singleton, so repeated in-process
        runs (e.g. the test suite) must not accumulate into each other."""
        p = Profiler()
        p.reset(enabled=True)
        with p.phase("first"):
            pass
        p.reset(enabled=True)
        with p.phase("second"):
            pass
        p.stop()
        report = p.report()
        assert "second" in report
        assert "first" not in report

    def test_nested_phases_are_indented_not_summed(self):
        """Inner phases are measured inside their parent, so the report must
        signal nesting rather than presenting an additive column."""
        p = Profiler()
        p.reset(enabled=True)
        with p.phase("stage1.wall"):
            with p.phase("stage1.window_total"):
                with p.phase("stage1.dggs_index"):
                    pass
        p.stop()
        lines = p.report().splitlines()
        wall = next(ln for ln in lines if "stage1.wall" in ln)
        inner = next(ln for ln in lines if "dggs_index" in ln)
        # dggs_index is rendered more deeply indented than its ancestor.
        assert (len(inner) - len(inner.lstrip())) > (len(wall) - len(wall.lstrip()))


class TestParallelismIsCPUBased:
    """The parallelism figure must measure work, not elapsed time in a thread.

    Deriving it from summed worker *wall* time makes it tend towards the thread
    count whenever threads block, so it reads ~7x on runs that are slower than
    --threads 1.

    test_blocked_threads_are_not_reported_as_parallelism is the one that catches
    that directly: substituting worker wall time back into the formula fails it
    and nothing else. The rest guard the opposite error -- real work must not be
    reported as stalled, and a single thread must not trigger the warning.
    """

    @staticmethod
    def _threaded(work, n_threads: int) -> str:
        p = Profiler()
        p.reset(enabled=True)

        def worker():
            with p.phase("stage1.window_total"):
                work()

        with p.phase("stage1.wall"):
            threads = [threading.Thread(target=worker) for _ in range(n_threads)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
        p.stop()
        return p.report()

    @needs_thread_cpu
    def test_sleeping_costs_wall_time_but_not_cpu_time(self):
        p = Profiler()
        p.reset(enabled=True)
        with p.phase("stage1.window_total"):
            time.sleep(0.05)
        p.stop()

        row = next(
            ln
            for ln in p.report().splitlines()
            if ln.strip().startswith("window_total")
        )
        wall_s, cpu_s = row.split()[1], row.split()[2]
        assert float(wall_s) >= 0.04
        assert float(cpu_s) < 0.02

    @needs_thread_cpu
    def test_blocked_threads_are_not_reported_as_parallelism(self):
        """Four threads that only sleep have achieved nothing, however much
        thread-time they accumulate."""
        report = self._threaded(lambda: time.sleep(0.1), n_threads=4)

        assert _stat(report, "parallelism") < 0.5
        assert _stat(report, "thread stall") > 90.0

    @needs_thread_cpu
    def test_a_single_busy_thread_reads_as_fully_occupied(self):
        """The converse, and independent of how many cores are available: one
        thread doing real work must not be reported as stalled."""
        report = self._threaded(lambda: _burn_cpu(0.1), n_threads=1)

        assert _stat(report, "parallelism") > 0.8
        assert _stat(report, "thread stall") < 20.0

    @needs_thread_cpu
    def test_poor_parallelism_is_called_out_when_threads_were_requested(self):
        p = Profiler()
        p.reset(enabled=True)
        p.note("threads", 7)
        with p.phase("stage1.wall"):
            with p.phase("stage1.window_total"):
                time.sleep(0.05)
        p.stop()

        assert "not paying for themselves" in p.report()

    @needs_thread_cpu
    def test_no_warning_when_a_single_thread_was_requested(self):
        p = Profiler()
        p.reset(enabled=True)
        p.note("threads", 1)
        with p.phase("stage1.wall"):
            with p.phase("stage1.window_total"):
                time.sleep(0.05)
        p.stop()

        assert "not paying for themselves" not in p.report()


class TestProfileCLI(TestRunthrough):
    def setUp(self):
        super().setUp()
        self._raster = self.make_temp_raster(_make_raster)

    def _invoke(self, *extra):
        if TEST_OUTPUT_PATH.exists():
            self.clearOutFolder(TEST_OUTPUT_PATH)
        TEST_OUTPUT_PATH.mkdir(exist_ok=True)
        result = CliRunner().invoke(
            cli,
            [
                "h3",
                str(self._raster),
                str(TEST_OUTPUT_PATH),
                "-r",
                str(_RES),
                *extra,
            ],
            catch_exceptions=False,
        )
        self.assertEqual(result.exit_code, 0, result.output)
        return result

    def test_profile_flag_emits_a_report(self):
        result = self._invoke("--profile")
        self.assertIn("Profile", result.output)
        self.assertIn("stage1.wall", result.output)
        self.assertIn("wall clock", result.output)
        # Context that makes the timings interpretable
        self.assertIn("windows", result.output)
        self.assertIn("bands", result.output)
        if _THREAD_CPU is not None:
            self.assertIn("cpu", result.output)
            self.assertIn("Stage 1 parallelism", result.output)
            self.assertIn("Stage 1 thread stall", result.output)

    def test_without_flag_no_report_and_profiler_disabled(self):
        result = self._invoke()
        self.assertNotIn("Profile", result.output)
        self.assertFalse(PROFILER.enabled)

    def test_profiling_does_not_change_output(self):
        self._invoke()
        without = read_output(TEST_OUTPUT_PATH).to_pandas().sort_index()
        self._invoke("--profile")
        with_prof = read_output(TEST_OUTPUT_PATH).to_pandas().sort_index()
        self.assertEqual(list(without.columns), list(with_prof.columns))
        self.assertEqual(len(without), len(with_prof))
        self.assertTrue((without["band_1"].values == with_prof["band_1"].values).all())
