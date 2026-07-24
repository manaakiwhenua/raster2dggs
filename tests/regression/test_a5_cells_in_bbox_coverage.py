"""
Completeness regression test for A5's cells_in_bbox.

Background: the previous implementation (raster2dggs/indexers/a5rasterindexer.py) was a
hand-rolled top-down recursive descent that pruned branches using a flat, isotropic
degree-based radius applied identically to both latitude and longitude, with no
correction for meridian convergence (cos(latitude)). A degree of longitude covers far
less ground away from the equator than at it, so the pruning silently dropped
legitimate boundary cells at higher latitudes -- worse at finer resolution. See
CLAUDE.md for the full investigation.

tests/classes/test_cells_in_bbox.py only ever checked that returned cells are a *valid
subset* (non-empty, centroids inside bbox) -- never *completeness* -- which is exactly
why this went unnoticed. This test checks completeness: every cell a query point can
land in must be present, not just a subset of them.

cells_in_bbox itself calls a5.polygon_to_cells + uncompact, so using those same two
functions as this test's ground truth would only exercise wiring (does cells_in_bbox
build the ring and pass the right resolution to uncompact), not the completeness
property -- both sides of the assertion would be the same algorithm. Ground truth here
instead comes from densely sampling query points across (and a little beyond) each
bbox, mapping each to a candidate cell via a5.lonlat_to_cell, then keeping only the
candidates whose own centroid (via a5.cell_to_lonlat) is actually inside the bbox --
matching cells_in_bbox's documented centroid-based contract exactly, using only the
lonlat_to_cell/cell_to_lonlat primitives rather than polygon_to_cells's flood-fill
(a5/regions/polygon.py), so this exercises a genuinely different code path.

Sampling must extend a little beyond the bbox: a query point taken exactly on the edge
can land in a cell whose own centroid is just outside the bbox (an adjacent cell
straddling the boundary), which cells_in_bbox correctly excludes. The centroid filter
below, combined with padding the sample region outward, keeps that cell out of the
expected set too.
"""

import numpy as np
import pytest

try:
    import a5

    from raster2dggs.indexers.a5rasterindexer import A5RasterIndexer
except ImportError:
    pytest.skip("a5 extra not installed", allow_module_level=True)

_RESOLUTION = 9
_GRID_STEP = 0.02  # degrees; a resolution-9 cell spans roughly 0.1 degree
_PAD = _GRID_STEP * 5  # sample a bit beyond the bbox so boundary cells aren't missed

# Bboxes spanning multiple latitude bands. The high-latitude and near-polar ones
# reproduce the conditions under which the old heuristic was empirically confirmed
# (during investigation of the A5 --overlay "holes" bug) to drop cells -- thousands of
# misses across a broader latitude/longitude sweep at a nearby resolution.
_BBOXES = [
    (-0.5, -0.5, 0.5, 0.5),  # equator: sanity baseline
    (-0.5, 29.5, 0.5, 30.5),  # mid-latitude
    (-0.5, -30.5, 0.5, -29.5),
    (89.5, 59.5, 90.5, 60.5),  # high latitude
    (89.5, -60.5, 90.5, -59.5),
    (119.5, -85.5, 120.5, -84.5),  # near-polar
]


def _expected_cells(min_lon, min_lat, max_lon, max_lat, resolution, step=_GRID_STEP):
    """Every cell whose own centroid lies in the bbox, found via dense sampling."""
    lons = np.arange(min_lon - _PAD, max_lon + _PAD, step)
    lats = np.arange(min_lat - _PAD, max_lat + _PAD, step)
    candidates = {
        a5.lonlat_to_cell((lon, lat), resolution) for lon in lons for lat in lats
    }
    expected = set()
    for cell in candidates:
        clon, clat = a5.cell_to_lonlat(cell)
        clon = (clon + 180.0) % 360.0 - 180.0
        if min_lat <= clat <= max_lat and min_lon <= clon <= max_lon:
            expected.add(a5.u64_to_hex(cell))
    return expected


@pytest.fixture(scope="module")
def indexer():
    return A5RasterIndexer("a5")


@pytest.mark.parametrize("bbox", _BBOXES, ids=[str(b) for b in _BBOXES])
def test_cells_in_bbox_is_complete(indexer, bbox):
    """Every cell reachable by a dense point sample of the bbox must appear in
    cells_in_bbox's result."""
    min_lon, min_lat, max_lon, max_lat = bbox
    expected = _expected_cells(min_lon, min_lat, max_lon, max_lat, _RESOLUTION)
    actual = indexer.cells_in_bbox(min_lon, min_lat, max_lon, max_lat, _RESOLUTION)
    missing = expected - actual
    assert not missing, (
        f"cells_in_bbox is missing {len(missing)}/{len(expected)} cells for bbox "
        f"{bbox} at resolution {_RESOLUTION}: {sorted(missing)[:5]}"
    )
