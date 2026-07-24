"""
Regression test for DGGALRasterIndexer.cell_area_m2's GeoPoint argument order bug.

Background: dggal.GeoPoint's constructor is GeoPoint(lat, lon), not GeoPoint(lon, lat)
(confirmed via dggal.GeoPoint.__init__'s own signature). cell_area_m2 called
dggal.GeoPoint(lon, lat) -- swapped. For any point with |longitude| > 90 degrees (most
of the globe), the swapped call passes an out-of-range "latitude" (the real longitude
value) -- DGGAL doesn't raise on this, it silently resolves to a degenerate,
resolution-0 zone instead of the requested resolution, confirmed empirically (a
resolution-8 request returning an "I0-..." zone id).

Corrupts cell_area_m2 for all 16 DGGAL-backed grids, since it lives in the shared
DGGALRasterIndexer base class -- used directly, and to size --overlay's bbox-buffer
padding and by the resolution auto-detection modes (smaller-than-pixel/
larger-than-pixel/min-diff). Contrast with cells_in_bbox, this file's other GeoPoint
call site, which is already correctly ordered (ll=dggal.GeoPoint(min_lat, min_lon)).

Ground truth here independently re-derives the zone via a correctly-ordered
GeoPoint(lat, lon) call, so this doesn't just test cell_area_m2 against itself.
"""

import pyproj
import pytest
import shapely

try:
    import dggal

    from raster2dggs.indexers.dggalrasterindexer import (
        HEALPixRasterIndexer,
        ISEA4RRasterIndexer,
        ISEA9RRasterIndexer,
    )
except ImportError:
    pytest.skip("dggal extra not installed", allow_module_level=True)

_RESOLUTION = 8
# Points with |longitude| > 90: exactly where the swapped call produces an
# out-of-range "latitude" and silently collapses to a degenerate zone.
_POINTS = [
    (40.0, 100.0),
    (-33.0, 151.0),  # Sydney-ish
    (10.0, -120.0),
]


def _expected_area_m2(dggrs, resolution, lat, lon):
    zone = dggrs.getZoneFromWGS84Centroid(resolution, dggal.GeoPoint(lat, lon))
    geo_points = dggrs.getZoneRefinedWGS84Vertices(zone, 0)
    polygon = shapely.Polygon([(p.lon, p.lat) for p in geo_points])
    area_m2, _ = pyproj.Geod(ellps="WGS84").geometry_area_perimeter(polygon)
    return abs(area_m2)


@pytest.mark.parametrize(
    "indexer_cls,dggs_name",
    [
        (ISEA4RRasterIndexer, "isea4r"),
        (ISEA9RRasterIndexer, "isea9r"),
        (HEALPixRasterIndexer, "healpix"),
    ],
)
@pytest.mark.parametrize("lat,lon", _POINTS)
def test_cell_area_m2_uses_correct_geopoint_order(indexer_cls, dggs_name, lat, lon):
    indexer = indexer_cls(dggs_name)
    expected = _expected_area_m2(indexer.dggrs, _RESOLUTION, lat, lon)
    actual = indexer.cell_area_m2(_RESOLUTION, lat, lon)
    # NB these are equal-area grids, so a wrongly-swapped GeoPoint call still lands on
    # *some* same-resolution zone with a very similar (but not identical) area -- a loose
    # tolerance here would pass even against the bug. Once fixed, cell_area_m2's
    # computation is identical to _expected_area_m2's, so this should match exactly;
    # a tight relative tolerance guards against incidental floating-point noise only.
    assert actual == pytest.approx(expected, rel=1e-9)
