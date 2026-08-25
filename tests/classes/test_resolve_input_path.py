"""
Unit tests for common.resolve_input_path: local paths are returned as Path,
remote URIs and GDAL virtual filesystem (/vsi*) paths pass through as str,
and anything else raises FileNotFoundError.
"""

import pathlib

import pytest

from raster2dggs import common


def test_existing_local_path_returned_as_path(tmp_path):
    f = tmp_path / "raster.tif"
    f.touch()
    result = common.resolve_input_path(str(f))
    assert isinstance(result, pathlib.Path)
    assert result == f


def test_scheme_uri_passes_through_as_str():
    uri = "https://example.com/raster.tif"
    result = common.resolve_input_path(uri)
    assert isinstance(result, str)
    assert result == uri


@pytest.mark.parametrize(
    "vsi_path",
    [
        "/vsicurl/https://example.com/raster.tif",
        "/vsis3/bucket/raster.tif",
        "/vsizip//vsicurl/https://example.com/archive.zip/raster.tif",
        "/vsimem/raster.tif",
    ],
)
def test_vsi_path_passes_through_as_str(vsi_path):
    result = common.resolve_input_path(vsi_path)
    assert isinstance(result, str)
    assert result == vsi_path


def test_missing_local_path_raises():
    with pytest.raises(FileNotFoundError):
        common.resolve_input_path("/no/such/raster.tif")
