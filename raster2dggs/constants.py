import multiprocessing
import tempfile
from enum import StrEnum

MIN_H3, MAX_H3 = (0, 15)
MIN_RHP, MAX_RHP = (0, 15)
MIN_GEOHASH, MAX_GEOHASH = (1, 12)
MIN_MAIDENHEAD, MAX_MAIDENHEAD = (1, 6)
MIN_S2, MAX_S2 = (0, 30)
MIN_A5, MAX_A5 = (0, 30)
MIN_ISEA4R, MAX_ISEA4R = (
    0,
    25,
)  # dggrs.getMaxDGGRSZoneLevel
MIN_ISEA9R, MAX_ISEA9R = (
    0,
    16,
)
MIN_ISEA9R, MAX_ISEA9R = (
    0,
    16,
)
MIN_ISEA3H, MAX_ISEA3H = (
    0,
    33,
)
MIN_ISEA7H, MAX_ISEA7H = (
    0,
    19,
)
MIN_ISEA7H_Z7, MAX_ISEA7H_Z7 = (
    0,
    19,
)
MIN_IVEA4R, MAX_IVEA4R = (
    0,
    25,
)
MIN_IVEA9R, MAX_IVEA9R = {
    0,
    16,
}
MIN_IVEA3H, MAX_IVEA3H = (
    0,
    33,
)
MIN_IVEA7H, MAX_IVEA7H = (
    0,
    19,
)
MIN_IVEA7H_Z7, MAX_IVEA7H_Z7 = (
    0,
    19,
)
MIN_RTEA4R, MAX_RTEA4R = (
    0,
    25,
)
MIN_RTEA9R, MAX_RTEA9R = (
    0,
    16,
)
MIN_RTEA3H, MAX_RTEA3H = (
    0,
    33,
)
MIN_RTEA7H, MAX_RTEA7H = (
    0,
    19,
)
MIN_RTEA7H_Z7, MAX_RTEA7H_Z7 = (
    0,
    19,
)
MIN_HEALPIX, MAX_HEALPIX = (
    0,
    26,
)
MIN_RHEALPIX, MAX_RHEALPIX = (
    0,
    16,
)

DEFAULT_NAME: str = "value"

DEFAULTS = {
    "compression": "snappy",
    "threads": (multiprocessing.cpu_count() - 1),
    "aggfunc": "mean",
    "decimals": 1,
    "geo": "none",
    "tempdir": tempfile.tempdir,
    "nodata_policy": "omit",
}


def zero_padding(dggs: str) -> int:
    max_res_lookup = {
        "h3": MAX_H3,
        "rhp": MAX_RHP,
        "geohash": MAX_GEOHASH,
        "maidenhead": MAX_MAIDENHEAD,
        "s2": MAX_S2,
        "a5": MAX_A5,
        "isea4r": MAX_ISEA4R,
        "isea9r": MAX_ISEA9R,
        "isea3h": MAX_ISEA3H,
        "isea7h": MAX_ISEA7H,
        "isea7h_z7": MAX_ISEA7H_Z7,
        "ivea4r": MAX_IVEA4R,
        "ivea9r": MAX_IVEA9R,
        "ivea3h": MAX_IVEA3H,
        "ivea7h": MAX_IVEA7H,
        "ivea7h_z7": MAX_IVEA7H_Z7,
        "rtea4r": MAX_RTEA4R,
        "rtea9r": MAX_RTEA9R,
        "rtea3h": MAX_RTEA3H,
        "rtea7h": MAX_RTEA7H,
        "rtea7h_z7": MAX_RTEA7H_Z7,
        "healpix": MAX_HEALPIX,
        "rhealpix": MAX_RHEALPIX,
    }
    max_res = max_res_lookup.get(dggs)
    if max_res is None:
        raise ValueError(f"Unknown DGGS type: {dggs}")
    return len(str(max_res))


class ResolutionMode(StrEnum):
    SMALLER_THAN_PIXEL = "smaller-than-pixel"
    LARGER_THAN_PIXEL = "larger-than-pixel"
    MIN_DIFF = "min-diff"


class Semantics(StrEnum):
    POINT_CENTER_STRICT = "point_center_strict"
    POINT_SAMPLE_FIELD = "point_sample_field"
    CELL_AVERAGE = "cell_average"
    PIECEWISE_CONSTANT = "piecewise_constant"
    FRACTION_COVER = "fraction_cover"
    COUNT_TOTAL = "count_total"
    DENSITY = "density"
    EVENT_INDICATOR = "event_indicator"


class Transfer(StrEnum):
    ASSIGN_CENTERS = "assign_centers"
    SAMPLE_NN = "sample_nn"
    SAMPLE_INTERP = "sample_interp"
    OVERLAY_WEIGHTED = "overlay_weighted"
    OVERLAY_MODE = "overlay_mode"
    MASS_PRESERVE = "mass_preserve"


class OutputSchema(StrEnum):
    VALUE = "value"
    FRACTIONS = "fractions"
    HISTOGRAM = "histogram"
    LIST = "list"


INAPPROPRIATE: set = {
    # point_center_strict: only assign_centers is appropriate
    ("point_center_strict", "sample_nn"),
    ("point_center_strict", "sample_interp"),
    ("point_center_strict", "overlay_weighted"),
    ("point_center_strict", "overlay_mode"),
    ("point_center_strict", "mass_preserve"),
    # point_sample_field: assign_centers treats pixels as non-interpolatable points
    ("point_sample_field", "assign_centers"),
    # cell_average: must use area-weighted overlay
    ("cell_average", "assign_centers"),
    ("cell_average", "sample_nn"),
    ("cell_average", "sample_interp"),
    ("cell_average", "overlay_mode"),
    ("cell_average", "mass_preserve"),
    # piecewise_constant: assign_centers ignores pixel area; interp and mass wrong
    ("piecewise_constant", "assign_centers"),
    ("piecewise_constant", "sample_interp"),
    ("piecewise_constant", "mass_preserve"),
    # fraction_cover: values are areal fractions; point-based methods are wrong
    ("fraction_cover", "assign_centers"),
    ("fraction_cover", "sample_nn"),
    ("fraction_cover", "overlay_mode"),
    ("fraction_cover", "mass_preserve"),
    # count_total: totals require mass preservation; sampling breaks conservation
    ("count_total", "assign_centers"),
    ("count_total", "sample_nn"),
    ("count_total", "sample_interp"),
    ("count_total", "overlay_weighted"),
    ("count_total", "overlay_mode"),
    # density: per-area intensity; assign_centers and overlay_mode are wrong
    ("density", "assign_centers"),
    ("density", "overlay_mode"),
    ("density", "mass_preserve"),
    # event_indicator: interpolation is meaningless for discrete events
    ("event_indicator", "sample_interp"),
}

IMPLEMENTED: set = {
    ("point_center_strict", "assign_centers", "value"),
    ("point_center_strict", "assign_centers", "list"),
    ("point_center_strict", "assign_centers", "histogram"),
}


# Surface area of the WGS84 oblate spheroid in m²
# Formula: 2π a²(1 + (b²/a²e) atanh(e)), a=6378137.0 m, b≈6356752.314140 m, e=eccentricity
WGS84_SURFACE_AREA_M2: float = 5.10065621724088e14

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
    "majority",
    "nunique",
    "range",
]

GEOM_TYPES = ["point", "polygon", "none"]

NODATA_POLICY_OPTIONS = ["omit", "emit"]
