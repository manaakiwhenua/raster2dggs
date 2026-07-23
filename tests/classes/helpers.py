import numpy as np
import rasterio
from rasterio.crs import CRS
from rasterio.transform import from_bounds


def make_raster(
    path: str,
    bounds: tuple,
    size: int,
    pixel_value: float = 1.0,
    nodata: float = None,
) -> None:
    """Write a uniform single-band float32 WGS84 GeoTIFF for use in tests.

    If nodata is provided, pixel [0, 0] is set to nodata so tests can verify
    nodata handling without requiring a fully masked raster.
    """
    data = np.full((1, size, size), pixel_value, dtype=np.float32)
    if nodata is not None:
        data[0, 0, 0] = nodata
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        height=size,
        width=size,
        count=1,
        dtype="float32",
        crs=CRS.from_epsg(4326),
        transform=from_bounds(*bounds, size, size),
        nodata=nodata,
    ) as dst:
        dst.write(data)


def make_gradient_raster(
    path: str,
    bounds: tuple,
    size: int,
) -> None:
    """Write a single-band float32 WGS84 GeoTIFF with continuous, non-uniform
    values (0..size*size-1) for use in numeric histogram binning tests."""
    data = np.arange(size * size, dtype=np.float32).reshape(1, size, size)
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        height=size,
        width=size,
        count=1,
        dtype="float32",
        crs=CRS.from_epsg(4326),
        transform=from_bounds(*bounds, size, size),
    ) as dst:
        dst.write(data)
