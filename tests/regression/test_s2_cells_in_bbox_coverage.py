"""
Completeness regression test for S2's cells_in_bbox.

Background: A5 and rHEALPix's cells_in_bbox both had a confirmed bug where a
hand-rolled, isotropic degree-based radius (with no cos(latitude) correction
for meridian convergence) silently dropped legitimate boundary cells at
higher latitudes. S2's implementation (raster2dggs/indexers/s2rasterindexer.py)
is architecturally different -- it delegates the actual covering to
s2sphere.RegionCoverer, a real spherical-geometry library, rather than a
hand-rolled heuristic -- but cos(latitude) does appear in this file, in the
max_cells budget estimate passed to RegionCoverer (a flat-earth bbox-area
approximation). Whether an underestimated max_cells at a fixed
min_level == max_level can cause RegionCoverer to return an incomplete
covering was untested (test_cells_in_bbox.py only ever checks that returned
cells are a *valid subset*, never completeness). This test checks
completeness directly, across the same latitude sweep that caught the A5/
rHEALPix bugs.

Confirmed (by temporarily forcing max_cells=1 in cells_in_bbox and re-running
this test -- still passed): max_cells doesn't actually bound completeness
here. With min_level == max_level, RegionCoverer has no coarser level to
substitute cells from, so it returns every cell needed to cover the region
regardless of the budget hint -- this is safe by construction, not merely by
luck of the estimate being generous enough in practice.

Ground truth here uses only s2sphere's point<->cell primitives
(CellId.from_lat_lng(...).parent(resolution) and
LatLng.from_point(cell_id.to_point())), never RegionCoverer itself --
otherwise both sides of the assertion would be the same algorithm and this
would only exercise wiring, not the completeness property.
"""

import numpy as np
import pytest

try:
    import s2sphere

    from raster2dggs.indexers.s2rasterindexer import S2RasterIndexer
except ImportError:
    pytest.skip("s2 extra not installed", allow_module_level=True)

_RESOLUTION = 9
_GRID_STEP = 0.02  # degrees; matches the A5/rHEALPix coverage tests
_PAD = _GRID_STEP * 5  # sample a bit beyond the bbox so boundary cells aren't missed

# Same latitude sweep as test_a5_cells_in_bbox_coverage.py / test_rhp_cells_in_bbox_coverage.py.
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
        s2sphere.CellId.from_lat_lng(s2sphere.LatLng.from_degrees(lat, lon)).parent(
            resolution
        )
        for lon in lons
        for lat in lats
    }
    expected = set()
    for cell_id in candidates:
        ll = s2sphere.LatLng.from_point(cell_id.to_point())
        clat, clon = ll.lat().degrees, ll.lng().degrees
        if min_lat <= clat <= max_lat and min_lon <= clon <= max_lon:
            expected.add(cell_id.to_token())
    return expected


@pytest.fixture(scope="module")
def indexer():
    return S2RasterIndexer("s2")


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
