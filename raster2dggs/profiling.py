"""
Opt-in phase timing for the indexing pipeline, surfaced by ``--profile``.

Stage 1 runs every raster window inside a ``ThreadPoolExecutor``, and
``cProfile`` only instruments the thread it is enabled in, so it cannot see
that work. These phase timers can.

Usage:

    from raster2dggs.profiling import PROFILER

    with PROFILER.phase("stage1.dggs_index"):
        ...

Disabled (the default), ``phase()`` returns a shared no-op context manager, so
the cost on the normal path is one attribute check.

Each phase records elapsed wall time *and* the CPU time of the thread that ran
it. Both are needed: a thread blocked on the GDAL read lock or waiting for the
GIL keeps accumulating wall time, so summed worker wall time approaches the
thread count whenever threads are contending -- which is exactly when
parallelism is absent. CPU time excludes blocked time, so it is the figure that
can tell work apart from waiting.
"""

from __future__ import annotations

import threading
import time
from collections import defaultdict
from contextlib import contextmanager, nullcontext

# Stateless and therefore safe to share across threads: __enter__/__exit__ do
# nothing, so there is no per-use state to race on.
_NULL_CONTEXT = nullcontext()

# Per-thread CPU time. Documented as available on Linux and Windows, so treat
# it as optional: without it the report simply omits the CPU column and the
# figures derived from it.
try:
    time.thread_time()
except (AttributeError, OSError):  # pragma: no cover - platform dependent
    _THREAD_CPU = None
else:
    _THREAD_CPU = time.thread_time

# Phases in pipeline order, with nesting depth. Depth matters for reading the
# report correctly: inner phases are measured *inside* their parent, so their
# times must not be read as additive with it. Unknown phases are reported last
# at depth 0.
_PHASE_ORDER: tuple[tuple[str, int], ...] = (
    ("stage1.wall", 0),
    ("stage1.window_total", 1),
    ("stage1.read_block", 2),
    ("stage1.reshape", 2),
    ("stage1.reproject", 2),
    ("stage1.build_frame", 2),
    ("stage1.dggs_index", 2),
    ("stage1.cells_in_bbox", 2),
    ("stage1.cells_to_lonlat", 2),
    ("stage1.cell_polygons", 2),
    ("stage1.exactextract", 2),
    ("stage1.arrow_build", 2),
    ("stage1.parquet_write", 2),
    ("stage2.total", 0),
)
_PHASE_INDEX = {name: i for i, (name, _) in enumerate(_PHASE_ORDER)}
_PHASE_DEPTH = dict(_PHASE_ORDER)


class Profiler:
    """Accumulates wall time, thread CPU time and call counts per named phase.

    Stage 1 calls into this from several worker threads at once. Updates and
    the ``report()`` snapshot are taken under a lock, which is what keeps the
    accumulator correct on free-threaded builds where the GIL does not.

    Summed phase wall time exceeds the elapsed wall clock when worker threads
    overlap -- but also when they merely block, so ``report()`` derives its
    parallelism figure from CPU time and reports the gap between the two as
    stall.
    """

    def __init__(self) -> None:
        self.enabled: bool = False
        self._lock = threading.Lock()
        self._totals: dict[str, float] = defaultdict(float)
        self._cpu: dict[str, float] = defaultdict(float)
        self._counts: dict[str, int] = defaultdict(int)
        self._context: dict[str, object] = {}
        self._counters: dict[str, int] = {}
        self._wall_start: float | None = None
        self._wall_total: float | None = None

    def reset(self, enabled: bool = False) -> None:
        """Clear all state. Called once per run, before any worker starts.

        The module-level PROFILER is a singleton, so a reset per run keeps
        repeated in-process invocations (notably the test suite) independent.
        """
        with self._lock:
            self.enabled = enabled
            self._totals = defaultdict(float)
            self._cpu = defaultdict(float)
            self._counts = defaultdict(int)
            self._context = {}
            self._counters = {}
            self._wall_start = time.perf_counter() if enabled else None
            self._wall_total = None

    def stop(self) -> None:
        """Freeze the wall-clock measurement."""
        if self.enabled and self._wall_start is not None:
            self._wall_total = time.perf_counter() - self._wall_start

    def phase(self, name: str):
        """Time a block of work, attributing it to ``name``."""
        if not self.enabled:
            return _NULL_CONTEXT
        return self._timed(name)

    @contextmanager
    def _timed(self, name: str):
        start = time.perf_counter()
        cpu_start = _THREAD_CPU() if _THREAD_CPU else 0.0
        try:
            yield
        finally:
            elapsed = time.perf_counter() - start
            cpu = (_THREAD_CPU() - cpu_start) if _THREAD_CPU else 0.0
            with self._lock:
                self._totals[name] += elapsed
                self._cpu[name] += cpu
                self._counts[name] += 1

    def note(self, key: str, value) -> None:
        """Record a scalar fact about the run (window count, band count, ...)
        so the timings can be interpreted per-unit rather than in aggregate."""
        if not self.enabled:
            return
        with self._lock:
            self._context[key] = value

    def add(self, key: str, amount: int) -> None:
        """Accumulate a running total across windows and threads."""
        if not self.enabled:
            return
        with self._lock:
            self._counters[key] = self._counters.get(key, 0) + amount

    def report(self) -> str:
        """Render the collected measurements as a plain-text table."""
        with self._lock:
            totals = dict(self._totals)
            cpu = dict(self._cpu)
            counts = dict(self._counts)
            context = dict(self._context)
            counters = dict(self._counters)
            wall = self._wall_total

        if not totals and not context and not counters:
            return "Profile: nothing recorded."

        def sort_key(phase: str) -> tuple[int, str]:
            return (_PHASE_INDEX.get(phase, len(_PHASE_ORDER)), phase)

        lines = ["", "Profile"]
        if context or counters:
            for key in sorted(context):
                lines.append(f"  {key}: {context[key]}")
            for key in sorted(counters):
                lines.append(f"  {key}: {counters[key]:,}")
            read = counters.get("pixels_read")
            kept = counters.get("rows_indexed")
            if read and kept is not None:
                lines.append(f"  valid pixels: {100 * kept / read:.1f}% of those read")
            lines.append("")

        cpu_col = "cpu" if _THREAD_CPU else ""
        header = (
            f"  {'phase':<28}{'seconds':>10}{cpu_col:>10}"
            f"{'% wall':>9}{'calls':>9}{'ms/call':>12}"
        )
        lines.append(header)
        lines.append(f"  {'-' * (len(header) - 2)}")

        def row(
            label: str, seconds: float, thread_cpu: float | None, calls: int | None
        ) -> str:
            pct = f"{100 * seconds / wall:>8.1f}%" if wall else "        -"
            if calls:
                per_call = f"{1000 * seconds / calls:>12.3f}"
                calls_s = f"{calls:>9}"
            else:
                per_call, calls_s = "           -", "        -"
            if not _THREAD_CPU:
                cpu_s = ""
            elif thread_cpu is None:
                cpu_s = f"{'-':>10}"
            else:
                cpu_s = f"{thread_cpu:>10.3f}"
            return f"  {label:<28}{seconds:>10.3f}{cpu_s}{pct}{calls_s}{per_call}"

        ordered = sorted(totals, key=sort_key)
        for phase in ordered:
            seconds = totals[phase]
            depth = _PHASE_DEPTH.get(phase, 0)
            # Indentation encodes nesting: an indented phase is measured inside
            # the one above it, so sibling times are not additive with the
            # parent's and the column must not be read as a total.
            label = ("  " * depth) + phase.split(".", 1)[-1] if depth else phase
            lines.append(row(label, seconds, cpu.get(phase), counts[phase]))

            # After listing a parent's children, show whatever of the parent is
            # not covered by them. Without this, a lightly-instrumented path
            # (--overlay, --sample) looks like it spends almost no time
            # anywhere, when really most of its work simply isn't broken down.
            if phase == "stage1.window_total":
                kids = [p for p in ordered if _PHASE_DEPTH.get(p, 0) == 2]
                remainder = seconds - sum(totals[p] for p in kids)
                cpu_remainder = cpu.get(phase, 0.0) - sum(cpu.get(p, 0.0) for p in kids)
                if remainder > 0.05 * seconds:
                    lines.append(
                        row("    (not broken down)", remainder, cpu_remainder, None)
                    )

        lines.append(f"  {'-' * (len(header) - 2)}")
        if wall:
            pad = 10 if _THREAD_CPU else 0
            lines.append(f"  {'wall clock':<28}{wall:>10.3f}{'':>{pad}}{'  100.0%':>9}")

        lines.extend(self._thread_lines(totals, cpu, context))
        lines.append("")
        return "\n".join(lines)

    @staticmethod
    def _thread_lines(
        totals: dict[str, float], cpu: dict[str, float], context: dict[str, object]
    ) -> list[str]:
        """Report what Stage 1's worker threads actually achieved.

        Parallelism is CPU seconds per second of elapsed Stage 1 time. It must
        not be derived from summed worker *wall* time: a thread blocked on the
        GDAL read lock or waiting for the GIL keeps accumulating wall time, so
        that ratio tends towards the thread count precisely when the threads are
        achieving nothing.
        """
        stage1_wall = totals.get("stage1.wall")
        worker_wall = totals.get("stage1.window_total")
        if not stage1_wall or not worker_wall:
            return []

        lines = []
        if not _THREAD_CPU:  # pragma: no cover - platform dependent
            lines.append(
                f"  Stage 1 worker thread-time: {worker_wall:.3f}s in "
                f"{stage1_wall:.3f}s wall (includes time blocked, so it is not "
                f"a parallelism figure)"
            )
            return lines

        worker_cpu = cpu.get("stage1.window_total", 0.0)
        stalled = worker_wall - worker_cpu
        lines.append(
            f"  Stage 1 parallelism: {worker_cpu / stage1_wall:.2f}x "
            f"({worker_cpu:.3f}s worker CPU in {stage1_wall:.3f}s wall)"
        )
        if worker_wall > 0:
            lines.append(
                f"  Stage 1 thread stall: {100 * stalled / worker_wall:.1f}% "
                f"({stalled:.3f}s of {worker_wall:.3f}s thread-time blocked)"
            )
        threads = context.get("threads")
        if isinstance(threads, int) and threads > 1 and worker_cpu / stage1_wall < 1.5:
            lines.append(
                f"  ^ {threads} threads are not paying for themselves here; "
                f"compare against --threads 1"
            )
        return lines


PROFILER = Profiler()
