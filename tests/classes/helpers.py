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


def make_rgba_raster(
    path: str,
    bounds: tuple,
    size: int,
    pixel_value: int = 100,
    masked_cols: int | None = None,
) -> None:
    """Write a uint8 RGBA WGS84 GeoTIFF with no nodata value, coverage expressed
    solely by the alpha band -- the LINZ-mosaic pattern of issue #105.

    The leftmost ``masked_cols`` columns (default: half the width) have alpha 0
    and RGB (0, 0, 0); everywhere else alpha is 255 and RGB is ``pixel_value``.
    """
    from rasterio.enums import ColorInterp

    if masked_cols is None:
        masked_cols = size // 2
    data = np.full((4, size, size), pixel_value, dtype=np.uint8)
    data[3] = 255
    data[:, :, :masked_cols] = 0
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        height=size,
        width=size,
        count=4,
        dtype="uint8",
        crs=CRS.from_epsg(4326),
        transform=from_bounds(*bounds, size, size),
    ) as dst:
        dst.write(data)
        dst.colorinterp = (
            ColorInterp.red,
            ColorInterp.green,
            ColorInterp.blue,
            ColorInterp.alpha,
        )


def make_internal_mask_raster(
    path: str,
    bounds: tuple,
    size: int,
    pixel_value: float = 1.0,
    masked_cols: int | None = None,
) -> None:
    """Write a single-band float32 WGS84 GeoTIFF with no nodata value whose
    validity is carried by an internal (TIFF) mask band: the leftmost
    ``masked_cols`` columns (default: half) are masked, and hold 0.0."""
    if masked_cols is None:
        masked_cols = size // 2
    data = np.full((1, size, size), pixel_value, dtype=np.float32)
    data[:, :, :masked_cols] = 0.0
    mask = np.full((size, size), 255, dtype=np.uint8)
    mask[:, :masked_cols] = 0
    with rasterio.Env(GDAL_TIFF_INTERNAL_MASK=True):
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
            dst.write_mask(mask)
