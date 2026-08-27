"""
Compaction's sibling-uniformity check on multi-aggregation values.

With more than one ``-a`` aggregation each band value is a dict, and under
``-d 0`` the aggregates are nullable ``Int64``, so the ``std`` of a
single-pixel cell is ``pd.NA`` *inside* the dict. ``dict.__eq__`` then
evaluates ``pd.NA == <number>`` -> ``NA``, and needing a bool it raises
``TypeError: boolean value of NA is ambiguous``. Missing-vs-missing must count
as uniform, missing-vs-number as different, and neither may raise.
"""

import numpy as np
import pandas as pd
import pytest

from raster2dggs.indexerfactory import indexer_instance
from raster2dggs.indexers.rasterindexer import _col_is_uniform, _freeze


class TestFreeze:
    def test_missing_values_share_one_key(self):
        keys = {_freeze(pd.NA), _freeze(None), _freeze(np.nan), _freeze(float("nan"))}
        assert len(keys) == 1

    def test_missing_differs_from_number(self):
        assert _freeze(pd.NA) != _freeze(0)
        assert _freeze(np.nan) != _freeze(0.0)

    def test_dict_key_order_irrelevant(self):
        assert _freeze({"a": 1, "b": 2}) == _freeze({"b": 2, "a": 1})

    def test_nested_and_list_values_are_hashable(self):
        v = {"classes": [1, 2], "fractions": [0.5, np.nan], "meta": {"n": pd.NA}}
        hash(_freeze(v))
        assert _freeze(v) == _freeze(
            {"classes": [1, 2], "fractions": [0.5, float("nan")], "meta": {"n": None}}
        )


class TestColIsUniform:
    def test_scalars(self):
        assert _col_is_uniform(pd.Series([7.0, 7.0, 7.0]))
        assert not _col_is_uniform(pd.Series([7.0, 8.0]))
        assert _col_is_uniform(pd.Series([np.nan, np.nan]))
        assert not _col_is_uniform(pd.Series([np.nan, 1.0]))

    def test_identical_dicts(self):
        s = pd.Series([{"mean": 42, "std": 0, "min": 42}] * 3)
        assert _col_is_uniform(s)

    def test_differing_dicts(self):
        s = pd.Series([{"mean": 42, "std": 0}, {"mean": 41, "std": 0}])
        assert not _col_is_uniform(s)

    def test_identical_dicts_with_na(self):
        s = pd.Series([{"mean": 42, "std": pd.NA}] * 3)
        assert _col_is_uniform(s)

    def test_na_versus_number_is_not_uniform_and_does_not_raise(self):
        # The failing case: single-pixel sibling (std NA) beside a multi-pixel one.
        s = pd.Series([{"mean": 42, "std": pd.NA}, {"mean": 42, "std": 0}])
        assert not _col_is_uniform(s)
        # ...regardless of which comes first (dict identity shortcut must not matter).
        assert not _col_is_uniform(s.iloc[::-1])

    def test_nan_and_na_are_the_same_missing(self):
        s = pd.Series([{"std": pd.NA}, {"std": np.nan}, {"std": None}])
        assert _col_is_uniform(s)

    def test_lists(self):
        assert _col_is_uniform(pd.Series([[1.0, 2.0], [1.0, 2.0]]))
        assert not _col_is_uniform(pd.Series([[1.0, 2.0], [1.0, 3.0]]))

    def test_none_mixed_with_dicts(self):
        # Histogram mode can yield None for some cells.
        assert not _col_is_uniform(pd.Series([None, {"values": [1]}]))
        assert _col_is_uniform(pd.Series([None, None]))


def _h3_sibling_frame(band_values):
    """Seven res-11 siblings under one res-10 parent plus a straggler, as in
    tests/regression/test_uint64_cell_ids.py; band values are given per sibling."""
    import h3.api.numpy_int as h3i

    parent10 = int(h3i.latlng_to_cell(-41.0, 174.0, 10))
    children = [int(c) for c in h3i.cell_to_children(parent10, 11)]
    assert len(children) == len(band_values)
    straggler = int(h3i.latlng_to_cell(-41.2, 174.2, 11))
    p5 = np.uint64(h3i.cell_to_parent(parent10, 5))
    df = pd.DataFrame(
        {"h3_05": p5, "band_1": band_values + [{"mean": 1, "std": pd.NA}]},
        index=pd.Index(np.array(children + [straggler], dtype=np.uint64), name="h3_11"),
    )
    return df, parent10, straggler


class TestCompactionWithNAInDicts:
    def test_identical_na_dicts_compact(self):
        df, parent10, straggler = _h3_sibling_frame([{"mean": 42, "std": pd.NA}] * 7)
        out = indexer_instance("h3").compaction(df, 11, 5)
        assert {int(v) for v in out.index} == {parent10, straggler}
        assert out.loc[np.uint64(parent10), "band_1"] == {"mean": 42, "std": pd.NA}

    def test_na_beside_number_does_not_compact_and_does_not_raise(self):
        values = [{"mean": 42, "std": pd.NA}] * 6 + [{"mean": 42, "std": 0}]
        df, parent10, straggler = _h3_sibling_frame(values)
        out = indexer_instance("h3").compaction(df, 11, 5)
        assert len(out) == 8  # nothing compacted
        assert parent10 not in {int(v) for v in out.index}

    @pytest.mark.parametrize("missing", [np.nan, None])
    def test_nan_and_na_compact_together(self, missing):
        values = [{"mean": 42, "std": pd.NA}] * 4 + [{"mean": 42, "std": missing}] * 3
        df, parent10, straggler = _h3_sibling_frame(values)
        out = indexer_instance("h3").compaction(df, 11, 5)
        assert {int(v) for v in out.index} == {parent10, straggler}
