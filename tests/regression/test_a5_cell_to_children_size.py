"""
Regression test for A5RasterIndexer.cell_to_children_size's aperture-5 special case.

Background: A5 subdivides by aperture 5 from resolution 0 to 1 (12 pentagons -> 60
cells), and aperture 4 thereafter -- not a constant aperture-4 subdivision from
resolution 0. cell_area_m2 had this same wrong assumption (12*4**resolution cells)
and was fixed by using pya5.cell_area() directly instead of a hand-rolled formula
(see CLAUDE.md). cell_to_children_size had the identical bug
(4 ** (desired_resolution - cell_level) unconditionally, wrong whenever the span
crosses the resolution 0 -> 1 boundary) and is fixed the same way: using pya5's own
get_num_children(parent_resolution, child_resolution) API function instead of a
hand-rolled power calculation.
"""

import pytest

try:
    import a5

    from raster2dggs.indexers.a5rasterindexer import A5RasterIndexer
except ImportError:
    pytest.skip("a5 extra not installed", allow_module_level=True)


def _cell_at_resolution(resolution):
    cell = a5.get_res0_cells()[0]
    for res in range(1, resolution + 1):
        cell = a5.cell_to_children(cell, res)[0]
    return cell


@pytest.fixture(scope="module")
def indexer():
    return A5RasterIndexer("a5")


@pytest.mark.parametrize(
    "parent_res,child_res,expected",
    [
        (0, 1, 5),  # the aperture-5 special case: 12 pentagons -> 60 cells
        (0, 2, 20),
        (0, 0, 1),
        (1, 2, 4),  # aperture-4 everywhere else
        (5, 8, 64),
        (3, 3, 1),
    ],
)
def test_cell_to_children_size_matches_get_num_children(
    indexer, parent_res, child_res, expected
):
    cell = _cell_at_resolution(parent_res)
    assert (
        a5.get_num_children(parent_res, child_res) == expected
    )  # sanity on pya5 itself
    assert indexer.cell_to_children_size(cell, child_res) == expected
