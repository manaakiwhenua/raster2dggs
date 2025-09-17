from interfaces import RasterIndexer
from indexers import *

# Match DGGS name to indexer class name
indexer_lookup = {
    "h3": H3RasterIndexer,
    "rhp": RHPRasterIndexer,
    "geohash": GeohashRasterIndexer,
    "maidenhead": MaidenheadRasterIndexer,
    "s2": S2RasterIndexer,
    }

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
    return indexer()