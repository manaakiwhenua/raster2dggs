from importlib import import_module
from typing import Dict, Tuple, Type

from raster2dggs.interfaces import IRasterIndexer

INDEXER_LOOKUP: Dict[str, Tuple[str, str, str]] = {
    "h3": ("raster2dggs.indexers.h3rasterindexer", "H3RasterIndexer", "h3"),
    "rhp": ("raster2dggs.indexers.rhprasterindexer", "RHPRasterIndexer", "rhp"),
    "geohash": (
        "raster2dggs.indexers.geohashrasterindexer",
        "GeohashRasterIndexer",
        "geohash",
    ),
    "maidenhead": (
        "raster2dggs.indexers.maidenheadrasterindexer",
        "MaidenheadRasterIndexer",
        "maidenhead",
    ),
    "s2": ("raster2dggs.indexers.s2rasterindexer", "S2RasterIndexer", "s2"),
    "a5": ("raster2dggs.indexers.a5rasterindexer", "A5RasterIndexer", "a5"),
    "isea4r": (
        "raster2dggs.indexers.dggalrasterindexer",
        "ISEA4RRasterIndexer",
        "dggal",
    ),
    "isea9r": (
        "raster2dggs.indexers.dggalrasterindexer",
        "ISEA9RRasterIndexer",
        "dggal",
    ),
    "isea3h": (
        "raster2dggs.indexers.dggalrasterindexer",
        "ISEA3HRasterIndexer",
        "dggal",
    ),
    "isea7h": (
        "raster2dggs.indexers.dggalrasterindexer",
        "ISEA7HRasterIndexer",
        "dggal",
    ),
    "isea7h_z7": (
        "raster2dggs.indexers.dggalrasterindexer",
        "ISEA7HZ7RasterIndexer",
        "dggal",
    ),
    "ivea4r": (
        "raster2dggs.indexers.dggalrasterindexer",
        "IVEA4RRasterIndexer",
        "dggal",
    ),
    "ivea9r": (
        "raster2dggs.indexers.dggalrasterindexer",
        "IVEA9RRasterIndexer",
        "dggal",
    ),
    "ivea3h": (
        "raster2dggs.indexers.dggalrasterindexer",
        "IVEA3HRasterIndexer",
        "dggal",
    ),
    "ivea7h": (
        "raster2dggs.indexers.dggalrasterindexer",
        "IVEA7HRasterIndexer",
        "dggal",
    ),
    "ivea7h_z7": (
        "raster2dggs.indexers.dggalrasterindexer",
        "IVEA7HZ7RasterIndexer",
        "dggal",
    ),
    "rtea4r": (
        "raster2dggs.indexers.dggalrasterindexer",
        "RTEA4RRasterIndexer",
        "dggal",
    ),
    "rtea9r": (
        "raster2dggs.indexers.dggalrasterindexer",
        "RTEA9RRasterIndexer",
        "dggal",
    ),
    "rtea3h": (
        "raster2dggs.indexers.dggalrasterindexer",
        "RTEA3HRasterIndexer",
        "dggal",
    ),
    "rtea7h": (
        "raster2dggs.indexers.dggalrasterindexer",
        "RTEA7HRasterIndexer",
        "dggal",
    ),
    "rtea7h_z7": (
        "raster2dggs.indexers.dggalrasterindexer",
        "RTEA7HZ7RasterIndexer",
        "dggal",
    ),
    "healpix": (
        "raster2dggs.indexers.dggalrasterindexer",
        "HEALPixRasterIndexer",
        "dggal",
    ),
    "rhealpix": (
        "raster2dggs.indexers.dggalrasterindexer",
        "RHEALPixRasterIndexer",
        "dggal",
    ),
}


def indexer_instance(dggs: str) -> IRasterIndexer:
    try:
        module_name, class_name, extra = INDEXER_LOOKUP[dggs]
    except KeyError as e:
        raise ValueError(
            f"Unknown DGGS: '{dggs}'. Options: {sorted(INDEXER_LOOKUP)}"
        ) from e

    try:
        module = import_module(module_name)
    except ModuleNotFoundError as e:
        raise ImportError(
            f"Missing dependency '{e.name}' for backend '{dggs}'.\n"
            f"Install optional dependencies: pip install 'raster2dggs[{extra}]' "
            f"(or 'raster2dggs[all]')."
        ) from e
    indexer: Type[IRasterIndexer] = getattr(module, class_name)
    return indexer(dggs)
