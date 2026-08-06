# Benchmarks

[pytest-benchmark](https://pytest-benchmark.readthedocs.io/) times individual
pipeline functions over many rounds and reports min, median, mean and standard
deviation, so a change of a few percent can be told apart from noise. Use it to
answer *"did this function get faster?"*

```bash
pytest bench/
```

These are not collected by a plain `pytest` run: `testpaths` is `tests`, and
`bench/` sits outside it.

Inputs are synthetic and in-memory, so a benchmark measures the phase it names
rather than GDAL read throughput or the state of the page cache. Groups are
parametrized over the variable that actually moves the number — band count,
nodata fraction, pixels per cell — so the shape of the curve is visible, not
just one point on it.

| Group | Measures |
|---|---|
| `dggs cell assignment` | lon/lat to cell IDs, one case per indexer implementation |
| `stage 1 window, by band count` | the whole per-window path against band count |
| `stage 1 window, by nodata fraction` | whether nodata pixels really are dropped before indexing |
| `stage 1 window, reprojection` | the share of a window spent reprojecting pixel centres |
| `stage 2 groupby, by pixels per cell` | aggregation cost against how many pixels share a cell |
| `stage 2 groupby, by aggregation count` | the multi-aggregation struct path against the scalar one |
| `stage 2 groupby, decimal rounding` | the cost of `--decimals` |

### Comparing against a previous run

```bash
pytest bench/ --benchmark-autosave                  # saves to .benchmarks/
pytest bench/ --benchmark-compare                   # compare with the last save
pytest bench/ --benchmark-compare-fail=median:10%   # non-zero exit on a >10% regression
```

`.benchmarks/` is gitignored: the numbers are machine-specific, so a saved run
is only comparable with another run on the same machine. The last form is what
a CI performance gate would use, on a runner dedicated enough for the variance
to be tolerable.

Useful flags: `--benchmark-columns=min,median,stddev,rounds` to trim the table,
`-k` to select a group, `--benchmark-histogram` to write SVG plots.

## What this does not cover

A benchmark here times one function on synthetic input. It cannot tell you
which phase dominates a real run, because it has no GDAL read, no thread pool
and no Parquet write. For that, run the CLI itself with `--profile`, which
reports a phase breakdown plus the window count, band count, source tiling and
achieved Stage 1 concurrency for whatever raster and flags you actually used.

The two are complementary: `--profile` says where the time goes on one run of
real data, `pytest bench/` says whether a change to a specific function was an
improvement.
