"""
@author: ndemaio
"""

from raster2dggs.interfaces import IRasterIndexer

import raster2dggs.indexers.h3rasterindexer as h3rasterindexer
import raster2dggs.indexers.rhprasterindexer as rhprasterindexer
import raster2dggs.indexers.geohashrasterindexer as geohashrasterindexer
import raster2dggs.indexers.maidenheadrasterindexer as maidenheadrasterindexer
import raster2dggs.indexers.s2rasterindexer as s2rasterindexer
import raster2dggs.indexers.a5rasterindexer as a5rasterindexer
import raster2dggs.indexers.dggalrasterindexer as dggalrasterindexer

"""
Match DGGS name to indexer class name
"""
indexer_lookup = {
    "h3": h3rasterindexer.H3RasterIndexer,
    "rhp": rhprasterindexer.RHPRasterIndexer,
    "geohash": geohashrasterindexer.GeohashRasterIndexer,
    "maidenhead": maidenheadrasterindexer.MaidenheadRasterIndexer,
    "s2": s2rasterindexer.S2RasterIndexer,
    "a5": a5rasterindexer.A5RasterIndexer,
    "isea4r": dggalrasterindexer.ISEA4RRasterIndexer,
    "isea9r": dggalrasterindexer.ISEA9RRasterIndexer,
    "isea7h": dggalrasterindexer.ISEA7HRasterIndexer,
    "isea7h_z7": dggalrasterindexer.ISEA7HZ7RasterIndexer,
    "ivea4r": dggalrasterindexer.IVEA4RRasterIndexer,
    "ivea9r": dggalrasterindexer.IVEA9RRasterIndexer,
    "ivea7h": dggalrasterindexer.IVEA7HRasterIndexer,
    "ivea7h_z7": dggalrasterindexer.IVEA7HZ7RasterIndexer,
    "rtea4r": dggalrasterindexer.RTEA4RRasterIndexer,
    "rtea9r": dggalrasterindexer.RTEA9RRasterIndexer,
    "rtea7h": dggalrasterindexer.RTEA7HRasterIndexer,
    "rtea7h_z7": dggalrasterindexer.RTEA7HZ7RasterIndexer,
    "healpix": dggalrasterindexer.HEALPixRasterIndexer,
    "rhealpix": dggalrasterindexer.RHEALPixRasterIndexer,
}


"""
Looks up and instantiates an appropriate indexer class given a DGGS name
as defined in the list of click commands
"""


def indexer_instance(dggs: str) -> IRasterIndexer:
    # Create and return appropriate indexer instance
    indexer = indexer_lookup[dggs]
    return indexer(dggs)
