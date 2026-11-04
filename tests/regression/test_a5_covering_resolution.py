"""
Regression test: A5 cell enumeration returns cells at the requested resolution
and nothing else.

pya5 >= 1.0 returns polygon_to_cells output as a covering: compacted cells
followed by a compaction marker that records the resolution. uncompact reads
the resolution from that marker (it no longer takes one as an argument) and
drops the marker from its output. cells_in_bbox and cells_overlapping_bbox in
raster2dggs/indexers/a5rasterindexer.py therefore pass the covering to
uncompact whole, so that the marker is consumed rather than leaking into the
result as a bogus cell, and so that the expansion lands on the requested
resolution. This test pins both properties.
"""

import pytest

try:
    import a5

    from raster2dggs.indexers.a5rasterindexer import A5RasterIndexer
except ImportError:
    pytest.skip("a5 extra not installed", allow_module_level=True)

if not hasattr(a5, "is_compaction_marker"):
    pytest.skip("pya5 < 1.0 has no compaction markers", allow_module_level=True)

_BBOX = (174.5, -41.5, 175.0, -41.0)  # Wellington, NZ
_RESOLUTIONS = [2, 6, 9, 12]


@pytest.fixture(scope="module")
def indexer():
    return A5RasterIndexer("a5")


@pytest.mark.parametrize("resolution", _RESOLUTIONS)
@pytest.mark.parametrize("method", ["cells_in_bbox", "cells_overlapping_bbox"])
def test_enumeration_yields_only_cells_at_resolution(indexer, method, resolution):
    cells = getattr(indexer, method)(*_BBOX, resolution)
    if method == "cells_overlapping_bbox":
        # A bbox always touches at least one cell. Its centres can all lie
        # outside the bbox when the cells are larger than it (resolution 2
        # here), so cells_in_bbox is legitimately empty there.
        assert len(cells) > 0
    markers = [c for c in cells if a5.is_compaction_marker(c)]
    assert not markers, f"compaction marker leaked into {method}: {markers}"
    invalid = [c for c in cells if not a5.is_valid_cell(c)]
    assert not invalid, f"{method} returned non-cells: {invalid[:5]}"
    wrong_res = {a5.get_resolution(c) for c in cells} - {resolution}
    assert not wrong_res, f"{method} returned cells at resolutions {wrong_res}"


@pytest.mark.parametrize("resolution", _RESOLUTIONS)
def test_overlapping_is_superset_of_centres(indexer, resolution):
    inside = indexer.cells_in_bbox(*_BBOX, resolution)
    overlapping = indexer.cells_overlapping_bbox(*_BBOX, resolution)
    assert inside <= overlapping


def test_compaction_marker_is_not_a_valid_cell():
    """valid_set filters on is_valid_a5_cell, so a marker that reached it would
    be dropped rather than written out as a cell."""
    marker = a5.polygon_to_cells(
        [(174.5, -41.5), (175.0, -41.5), (175.0, -41.0), (174.5, -41.0)], 6
    )[-1]
    assert a5.is_compaction_marker(marker)
    assert not A5RasterIndexer.is_valid_a5_cell(marker)
