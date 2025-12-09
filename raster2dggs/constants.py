import multiprocessing
import tempfile

MIN_H3, MAX_H3 = 0, 15
MIN_RHP, MAX_RHP = 0, 15
MIN_GEOHASH, MAX_GEOHASH = 1, 12
MIN_MAIDENHEAD, MAX_MAIDENHEAD = 1, 6
MIN_S2, MAX_S2 = 0, 30
MIN_A5, MAX_A5 = 0, 30
MIN_ISEA9R, MAX_ISEA9R = (
    0,
    18,
)  # NB correct for packing into 64 bit integer? see https://dggal.org/docs/html/dggal/Classes/DGGRS/Virtual%20Methods/getMaxDGGRSZoneLevel.html
MIN_ISEA7H, MAX_ISEA7H = (
    0,
    18,
)  # NB correct?

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
    "isea9r": lambda resolution: max(
        MIN_ISEA9R, (resolution - 5)
    ),  # NB use 5 for IS/VEA9R, and 10 for IS/VEA3H, and 8 for GNOSIS --- corresponds to ~64K sub-zones
    "isea7h": lambda resolution: max(MIN_ISEA7H, (resolution - DEFAULT_PARENT_OFFSET)),
}


def zero_padding(dggs: str) -> int:
    max_res_lookup = {
        "h3": MAX_H3,
        "rhp": MAX_RHP,
        "geohash": MAX_GEOHASH,
        "maidenhead": MAX_MAIDENHEAD,
        "s2": MAX_S2,
        "a5": MAX_A5,
        "isea9r": MAX_ISEA9R,
        "isea7h": MAX_ISEA7H,
    }
    max_res = max_res_lookup.get(dggs)
    if max_res is None:
        raise ValueError(f"Unknown DGGS type: {dggs}")
    return len(str(max_res))


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
