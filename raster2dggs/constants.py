import multiprocessing
import tempfile

MIN_H3, MAX_H3 = 0, 15
MIN_RHP, MAX_RHP = 0, 15
MIN_GEOHASH, MAX_GEOHASH = 1, 12
MIN_MAIDENHEAD, MAX_MAIDENHEAD = 1, 6
MIN_S2, MAX_S2 = 0, 30

DEFAULT_NAME: str = "value"

DEFAULTS = {
    "upscale": 1,
    "compression": "snappy",
    "threads": (multiprocessing.cpu_count() - 1),
    "aggfunc": "mean",
    "decimals": 1,
    "warp_mem_limit": 12000,
    "resampling": "average",
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
}
