"""
rhp base cells are resolution 0, not 1 (len(suid) == res + 1).

cell_area_m2 assumed otherwise and was 9x high from res 1 up, shifting the
auto-resolution modes a level; cell_to_children_size special-cased base cells as
6 * 9**(R - 1) rather than 9**R, so --compact could never merge a complete base
cell. Both stay hand-rolled arithmetic to avoid locking against the non-thread-safe
WGS84_003 (see test_rhp_thread_safety.py); this pins that arithmetic to the
library's own answers, where the lock costs nothing.
"""

import math

import pytest

try:
    from rhealpixdggs.dggs import WGS84_003

    from raster2dggs import constants as const
    from raster2dggs.indexers.rhprasterindexer import RHPRasterIndexer
except ImportError:  # pragma: no cover - exercised only without the rhp extra
    pytest.skip("rhp extra not installed", allow_module_level=True)


@pytest.fixture(scope="module")
def indexer():
    return RHPRasterIndexer("rhp")


def test_base_cells_are_resolution_zero():
    """The premise the arithmetic rests on."""
    assert WGS84_003.num_cells(0) == 6
    assert [str(c) for c in WGS84_003.grid(0)] == ["N", "O", "P", "Q", "R", "S"]
    assert all(len(str(c)) == 1 for c in WGS84_003.grid(0))


@pytest.mark.parametrize("resolution", range(0, 12))
def test_cell_area_matches_the_library(indexer, resolution):
    """Must agree with the library at every resolution; was 9x high."""
    expected = WGS84_003.cell_area(resolution, plane=False)
    got = indexer.cell_area_m2(resolution, lat=-41.0, lon=174.0)
    assert got == pytest.approx(expected, rel=1e-9)


def test_cell_area_at_resolution_zero_is_a_base_cell_not_the_globe(indexer):
    """Resolution 0 is one of six faces; there is no whole-globe cell."""
    got = indexer.cell_area_m2(0, lat=0.0, lon=0.0)
    assert got == pytest.approx(const.WGS84_SURFACE_AREA_M2 / 6, rel=1e-12)
    assert got < const.WGS84_SURFACE_AREA_M2


def test_cell_area_matches_an_observed_cell_count(indexer):
    """Cross-check against an observed cell count, not another formula."""
    resolution = 11
    min_lon, min_lat = 174.0, -41.0
    max_lon, max_lat = 174.019, -41.0144  # ~1.6 x 1.6 km at this latitude
    cells = indexer.cells_in_bbox(min_lon, min_lat, max_lon, max_lat, resolution)
    assert cells, "fixture bbox produced no cells"

    R = const.EARTH_MEAN_RADIUS_M
    bbox_m2 = (
        math.radians(max_lon - min_lon)
        * R**2
        * abs(math.sin(math.radians(max_lat)) - math.sin(math.radians(min_lat)))
    )
    implied = bbox_m2 / len(cells)
    assert implied == pytest.approx(
        indexer.cell_area_m2(resolution, 0.0, 0.0), rel=0.15
    )


@pytest.mark.parametrize(
    "cell,desired_resolution,expected",
    [
        ("N", 0, 1),  # a cell is its own sole descendant at its own resolution
        ("N", 1, 9),  # a base cell is resolution 0: 9 children, not 6 * 9**0
        ("N", 2, 81),
        ("N", 3, 729),
        ("N1", 1, 1),
        ("N1", 2, 9),
        ("N12", 4, 81),
        ("N1", 0, 0),  # no descendants above a cell's own resolution
        ("N12", 1, 0),
    ],
)
def test_cell_to_children_size(indexer, cell, desired_resolution, expected):
    assert indexer.cell_to_children_size(cell, desired_resolution) == expected


@pytest.mark.parametrize("resolution", range(1, 6))
def test_children_of_all_base_cells_tile_the_resolution(indexer, resolution):
    """Six base cells' descendants must account for every cell at a resolution."""
    per_base = indexer.cell_to_children_size("N", resolution)
    assert 6 * per_base == WGS84_003.num_cells(resolution)


def test_compacting_a_complete_base_cell_is_reachable(indexer):
    """A resolution-0 parent's expected_count must equal its true child count."""
    resolution = 2
    children = [str(c) for c in WGS84_003.grid(resolution) if str(c).startswith("N")]
    assert indexer.expected_count("N", resolution) == len(children)
