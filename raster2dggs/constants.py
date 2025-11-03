import multiprocessing
import tempfile

MIN_H3, MAX_H3 = 0, 15
MIN_RHP, MAX_RHP = 0, 15
MIN_GEOHASH, MAX_GEOHASH = 1, 12
MIN_MAIDENHEAD, MAX_MAIDENHEAD = 1, 6
MIN_S2, MAX_S2 = 0, 30
MIN_A5, MAX_A5 = 0, 30

DEFAULT_NAME: str = "value"

DEFAULTS = {
    "upscale": 1,
    "compression": "snappy",
    "threads": (multiprocessing.cpu_count() - 1),
    "aggfunc": "mean",
    "decimals": 1,
    "warp_mem_limit": 12000,
    "resampling": "average",
    "geo": "none",
    "tempdir": tempfile.tempdir,
}

DEFAULT_PARENT_OFFSET = 6

DEFAULT_DGGS_PARENT_RES = {
    "h3": lambda resolution: max(MIN_H3, (resolution - DEFAULT_PARENT_OFFSET)),
    "rhp": lambda resolution: max(MIN_RHP, (resolution - DEFAULT_PARENT_OFFSET)),
    "geohash": lambda resolution: max(
        MIN_GEOHASH, (resolution - DEFAULT_PARENT_OFFSET)
    ),
    "maidenhead": lambda resolution: MIN_MAIDENHEAD,
    "s2": lambda resolution: max(MIN_S2, (resolution - DEFAULT_PARENT_OFFSET)),
    "a5": lambda resolution: max(MIN_A5, (resolution - DEFAULT_PARENT_OFFSET)),
}


def zero_padding(dggs: str) -> int:
    max_res_lookup = {
        "h3": MAX_H3,
        "rhp": MAX_RHP,
        "geohash": MAX_GEOHASH,
        "maidenhead": MAX_MAIDENHEAD,
        "s2": MAX_S2,
        "a5": MAX_A5,
    }
    max_res = max_res_lookup.get(dggs)
    if max_res is None:
        raise ValueError(f"Unknown DGGS type: {dggs}")
    return len(str(max_res))


OPTION_HELP = {
    "resolution": lambda dggs: f"{dggs} resolution to index",
    "parent_res": lambda dggs, default: f"{dggs} parent resolution to index and aggregate to. Defaults to {default}",
    "band": "Band(s) to include in the output. Can specify multiple, e.g. `-b 1 -b 2 -b 4` for bands 1, 2, and 4 (all unspecified bands are ignored). If unused, all bands are included in the output (this is the default behaviour). Bands can be specified as numeric indices (1-based indexing) or string band labels (if present in the input), e.g. -b B02 -b B07 -b B12.",
    "upscale": "Upscaling factor, used to upsample input data on the fly; useful when the raster resolution is lower than the target DGGS resolution. Default (1) applies no upscaling. The resampling method controls interpolation.",
    "compression": "Compression method to use for the output Parquet files. Options include 'snappy', 'gzip', 'brotli', 'lz4', 'zstd', etc. Use 'none' for no compression.",
    "threads": "Number of threads to use when running in parallel. The default is determined based dynamically as the total number of available cores, minus one.",
    "aggfunc": "Numpy aggregate function to apply when aggregating cell values after DGGS indexing, in case of multiple pixels mapping to the same DGGS cell.",
    "decimals": "Number of decimal places to round values when aggregating. Use 0 for integer output.",
    "warp_mem_limit": "Input raster may be warped to EPSG:4326 if it is not already in this CRS. This setting specifies the warp operation's memory limit in MB.",
    "resampling": "Input raster may be warped to EPSG:4326 if it is not already in this CRS. Or, if the upscale parameter is greater than 1, there is a need to resample. This setting specifies this resampling algorithm.",
    "compact": "Compact the cells up to the parent resolution. Compaction is not applied for cells without identical values across all bands.",
    "geo": "Write output as a GeoParquet (v1.1.0) with either point or polygon geometry.",
    "tempdir": "Temporary data is created during the execution of this program. This parameter allows you to control where this data will be written.",
}

AGGFUNC_OPTIONS = [
    "count",
    "mean",
    "sum",
    "prod",
    "std",
    "var",
    "min",
    "max",
    "median",
    "mode",
]

GEOM_TYPES = ["point", "polygon", "none"]
