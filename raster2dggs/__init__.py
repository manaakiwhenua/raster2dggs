from importlib.metadata import PackageNotFoundError, version

try:
    __version__: str = version("raster2dggs")
except PackageNotFoundError:
    __version__: str = "unknown"
