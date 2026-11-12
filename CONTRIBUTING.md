# Contributing to raster2dggs

Contributions are welcome: bug reports, questions, documentation fixes, and code.

## Reporting a problem

Open an issue at <https://github.com/manaakiwhenua/raster2dggs/issues>. A report is much easier to act on if it includes:

- the `raster2dggs` version (`raster2dggs --version`), Python version and operating system;
- the full command you ran;
- the full error output, or a description of what was wrong with the result;
- the output of `gdalinfo` for the input raster or, better, a small raster that reproduces the problem.

## Asking a question or getting help

Use the same issue tracker. Questions about usage, about which transfer strategy suits your data, and requests for new grids or options are all fine there.

## Contributing code

For anything larger than a small fix, please open an issue first so the approach can be agreed before you spend time on it.

1. Fork the repository and create a branch from `master`.
2. Set up a development environment as described under [For development](README.md#for-development) in the README. The tests need every backend and the development tools: `poetry install --with dev --all-extras`. The `gdal` Python package must match the GDAL library on your system; continuous integration uses GDAL 3.13.1 from conda-forge.
3. Make your change, with tests.
4. Run the checks that continuous integration runs:

   ```bash
   black --check .
   ruff check .
   python -m pytest
   ```

5. Open a pull request against `master` describing what changed and why.

### Tests

- General feature tests live in `tests/classes/`. A test for a specific fixed bug belongs in `tests/regression/`.
- Behaviour that should hold for every grid should be tested across grids, not for one grid alone. `tests/classes/test_cells_in_bbox.py` shows how to derive the list of grids from `indexerfactory.INDEXER_LOOKUP`.
- Benchmarks in `bench/` are not part of the test suite and are not run in continuous integration.

### Adding a grid

A new DGGS backend needs an indexer class implementing `IRasterIndexer` (`raster2dggs/interfaces.py`), an entry in `INDEXER_LOOKUP` (`raster2dggs/indexerfactory.py`), a `DGGS_Spec` in `raster2dggs/cli_factory.py`, and an optional-dependency extra in `pyproject.toml`.

## Licence

raster2dggs is licensed under LGPL-3.0-or-later. By contributing you agree that your contribution is licensed under the same terms.
