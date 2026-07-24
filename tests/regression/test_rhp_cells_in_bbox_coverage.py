"""
Completeness regression test for rHEALPix's cells_in_bbox.

Background: the previous implementation tried rhealpixdggs's CellZoneFromPoly
first, falling back to a hand-rolled recursive nucleus search whenever
CellZoneFromPoly raised AttributeError -- known to happen for bboxes whose
rHEALPix containing-cell has vertices on the +/-180 degree antimeridian (e.g.
New Zealand, Pacific Ocean). That fallback used the same isotropic
degree-based radius heuristic as A5's old cells_in_bbox, with no cos(latitude)
correction on the longitude margin -- the same bug class, in the one place
(NZ-adjacent bboxes) where CellZoneFromPoly couldn't be used at all. See
CLAUDE.md for the full investigation.

cells_in_bbox now calls rhealpixdggs's own polyfill (which uses
cells_from_region, a different native function from CellZoneFromPoly, and
succeeds exactly where CellZoneFromPoly raised) for every bbox, so there's no
longer a fallback code path at all. Ground truth here comes from densely
sampling query points across (and a little beyond) each bbox, mapping each to
a candidate cell via rhpw.geo_to_rhp directly, then keeping only the
candidates whose own centroid (via rhpw.rhp_to_geo) is actually inside the
bbox -- independent of polyfill/cells_from_region, so this is a genuine
cross-check rather than testing cells_in_bbox against itself.
"""

import numpy as np
import pytest

try:
    import rhealpixdggs.rhp_wrappers as rhpw

    from raster2dggs.indexers.rhprasterindexer import RHPRasterIndexer
except ImportError:
    pytest.skip("rhp extra not installed", allow_module_level=True)

_RESOLUTION = 6
_GRID_STEP = 0.05  # degrees
_PAD = _GRID_STEP * 5  # sample a bit beyond the bbox so boundary cells aren't missed

# Bboxes spanning multiple latitude bands, including the antimeridian-adjacent
# NZ case that used to trigger CellZoneFromPoly's AttributeError fallback.
_BBOXES = [
    (-0.5, -0.5, 0.5, 0.5),  # equator: sanity baseline
    (-0.5, 29.5, 0.5, 30.5),  # mid-latitude
    (-0.5, -60.5, 0.5, -59.5),
    (89.5, 59.5, 90.5, 60.5),  # high latitude
    (89.5, -85.5, 90.5, -84.5),  # near-polar
    (172.0, -42.0, 175.0, -40.0),  # NZ: triggered the old AttributeError fallback
    (178.5, -40.5, 179.9, -39.5),  # right up against the antimeridian
]


def _expected_cells(min_lon, min_lat, max_lon, max_lat, resolution, step=_GRID_STEP):
    """Every cell whose own centroid lies in the bbox, found via dense sampling."""
    lons = np.arange(min_lon - _PAD, max_lon + _PAD, step)
    lats = np.arange(min_lat - _PAD, max_lat + _PAD, step)
    candidates = set()
    for lon in lons:
        for lat in lats:
            c = rhpw.geo_to_rhp(lat, lon, resolution, plane=False)
            if c is not None:
                candidates.add(c)
    expected = set()
    for c in candidates:
        clon, clat = rhpw.rhp_to_geo(c, plane=False)
        if min_lat <= clat <= max_lat and min_lon <= clon <= max_lon:
            expected.add(c)
    return expected


@pytest.fixture(scope="module")
def indexer():
    return RHPRasterIndexer("rhp")


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
