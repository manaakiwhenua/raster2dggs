"""
@author: ndemaio
"""

from raster2dggs.interfaces import RasterIndexer

import raster2dggs.indexers.h3rasterindexer as h3rasterindexer
import raster2dggs.indexers.rhprasterindexer as rhprasterindexer
import raster2dggs.indexers.geohashrasterindexer as geohashrasterindexer
import raster2dggs.indexers.maidenheadrasterindexer as maidenheadrasterindexer
import raster2dggs.indexers.s2rasterindexer as s2rasterindexer

'''
Match DGGS name to indexer class name
'''
indexer_lookup = {
    "h3": h3rasterindexer.H3RasterIndexer,
    "rhp": rhprasterindexer.RHPRasterIndexer,
    "geohash": geohashrasterindexer.GeohashRasterIndexer,
    "maidenhead": maidenheadrasterindexer.MaidenheadRasterIndexer,
    "s2": s2rasterindexer.S2RasterIndexer,
    }


'''
Looks up and instantiates an appropriate indexer class given a DGGS name
as defined in the list of click commands
'''
def indexer_instance(dggs: str) -> RasterIndexer:
    # Create and return appropriate indexer instance
    # Raise an exception for unsupported DGGS names
    if not dggs in indexer_lookup.keys():
        raise RuntimeError(
            "Unknown dggs {dggs}) -  must be one of [ {options} ]".format(
                dggs=dggs, options=", ".join(indexer_lookup.keys())
            )
        )
        
    indexer = indexer_lookup[dggs]
    return indexer(dggs)