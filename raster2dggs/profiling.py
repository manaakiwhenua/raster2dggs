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
"""

from __future__ import annotations

import threading
import time
from collections import defaultdict
from contextlib import contextmanager, nullcontext

# Stateless and therefore safe to share across threads: __enter__/__exit__ do
# nothing, so there is no per-use state to race on.
_NULL_CONTEXT = nullcontext()

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
    """Accumulates wall-clock time and call counts per named phase.

    Stage 1 calls into this from several worker threads at once. Updates and
    the ``report()`` snapshot are taken under a lock, which is what keeps the
    accumulator correct on free-threaded builds where the GIL does not.

    Summed phase time exceeds the wall clock when worker threads overlap;
    ``report()`` turns that into an explicit concurrency figure.
    """

    def __init__(self) -> None:
        self.enabled: bool = False
        self._lock = threading.Lock()
        self._totals: dict[str, float] = defaultdict(float)
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
        try:
            yield
        finally:
            elapsed = time.perf_counter() - start
            with self._lock:
                self._totals[name] += elapsed
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

        header = (
            f"  {'phase':<28}{'seconds':>10}{'% wall':>9}{'calls':>9}{'ms/call':>12}"
        )
        lines.append(header)
        lines.append(f"  {'-' * (len(header) - 2)}")

        def row(label: str, seconds: float, calls: int | None) -> str:
            pct = f"{100 * seconds / wall:>8.1f}%" if wall else "        -"
            if calls:
                per_call = f"{1000 * seconds / calls:>12.3f}"
                calls_s = f"{calls:>9}"
            else:
                per_call, calls_s = "           -", "        -"
            return f"  {label:<28}{seconds:>10.3f}{pct}{calls_s}{per_call}"

        ordered = sorted(totals, key=sort_key)
        for phase in ordered:
            seconds = totals[phase]
            depth = _PHASE_DEPTH.get(phase, 0)
            # Indentation encodes nesting: an indented phase is measured inside
            # the one above it, so sibling times are not additive with the
            # parent's and the column must not be read as a total.
            label = ("  " * depth) + phase.split(".", 1)[-1] if depth else phase
            lines.append(row(label, seconds, counts[phase]))

            # After listing a parent's children, show whatever of the parent is
            # not covered by them. Without this, a lightly-instrumented path
            # (--overlay, --sample) looks like it spends almost no time
            # anywhere, when really most of its work simply isn't broken down.
            if phase == "stage1.window_total":
                children = sum(
                    totals[p] for p in ordered if _PHASE_DEPTH.get(p, 0) == 2
                )
                remainder = seconds - children
                if remainder > 0.05 * seconds:
                    lines.append(row("    (not broken down)", remainder, None))

        lines.append(f"  {'-' * (len(header) - 2)}")
        if wall:
            lines.append(f"  {'wall clock':<28}{wall:>10.3f}{'  100.0%':>9}")

        # Effective Stage 1 concurrency: work done across all worker threads
        # divided by the wall time Stage 1 actually took. ~1.0 means the
        # threads achieved nothing (GIL-bound or lock-serialised); ~N means
        # N-way parallelism was realised.
        stage1_wall = totals.get("stage1.wall")
        stage1_work = totals.get("stage1.window_total")
        if stage1_wall and stage1_work:
            lines.append(
                f"  Stage 1 concurrency: {stage1_work / stage1_wall:.2f}x "
                f"({stage1_work:.3f}s of work in {stage1_wall:.3f}s wall)"
            )
        lines.append("")
        return "\n".join(lines)


PROFILER = Profiler()
