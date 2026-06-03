#!/usr/bin/env python3
"""
Generate a small suite of synthetic thematic GeoTIFF rasters with differing
"semantics" and nodata complexities for testing Raster→DGGS tooling.

Outputs (by default in ./sample_rasters):
  1) landcover_utm33.tif          (categorical, piecewise-constant, nodata holes/stripe)
  2) frac_treecover_utm33.tif     (fractional cover 0..1, nodata coast-like mask)
  3) popcount_webmerc.tif         (count_total, mass-in-cell, skewed totals, nodata polygon)
  4) temp_mean_wgs84.tif          (cell_average-like continuous, geographic CRS, nodata band)
  5) zone_ids_laea.tif            (categorical zones, large polygons + small islands, nodata)
  6) multiband_per_band_nodata_wgs84.tif  (4-band, per-band nodata at different pixels)
  7) swath_wgs84.tif              (WGS84 diagonal strip; bbox stress test via geodesic nodata mask)
  8) swath_polar_stereo.tif       (polar-stereographic CRS; rectangular grid appears as curved arc in WGS84)
  9) swath_u_shape.tif            (U-shape bbox stress test; corner-only bbox misses ~10° of southward extent)

Neutral landscape models (NLMs): uses smoothed noise (fractal-ish via multi-octave
upsampling + Gaussian smoothing) to create spatial autocorrelation.

Dependencies: numpy, rasterio
Optional: scipy (for gaussian filter). If scipy isn't installed, a simple box blur fallback is used.

Usage:
  python make_sample_rasters.py --outdir sample_rasters --seed 7
"""

import os
import argparse
import math
import numpy as np

import rasterio
from rasterio.transform import from_origin
from rasterio.crs import CRS

try:
    from scipy.ndimage import gaussian_filter

    HAVE_SCIPY = True
except Exception:
    HAVE_SCIPY = False


def box_blur(img, k=5, passes=2):
    """Fallback blur if scipy isn't available. Crude but OK for test rasters."""
    out = img.astype(np.float32)
    pad = k // 2
    for _ in range(passes):
        padded = np.pad(out, pad, mode="edge")
        acc = np.zeros_like(out, dtype=np.float32)
        for dy in range(k):
            for dx in range(k):
                acc += padded[dy : dy + out.shape[0], dx : dx + out.shape[1]]
        out = acc / (k * k)
    return out


def smooth(img, sigma):
    if HAVE_SCIPY:
        return gaussian_filter(img, sigma=sigma, mode="nearest")
    # approximate sigma with a box kernel size
    k = max(3, int(round(sigma * 3)) | 1)  # odd
    passes = 2
    return box_blur(img, k=k, passes=passes)


def nlm_fractal(shape, rng, octaves=5, persistence=0.55):
    """
    Simple multi-octave smooth noise producing spatial autocorrelation.
    Returns float32 in ~[0,1].
    """
    h, w = shape
    field = np.zeros((h, w), dtype=np.float32)
    amp = 1.0
    amp_sum = 0.0

    for o in range(octaves):
        step = 2 ** (octaves - o - 1)

        # CEIL so that coarse.repeat(step) is >= target size
        ch = max(2, int(math.ceil(h / step)))
        cw = max(2, int(math.ceil(w / step)))

        coarse = rng.random((ch, cw), dtype=np.float32)

        up = coarse.repeat(step, axis=0).repeat(step, axis=1)
        up = up[:h, :w]  # safe now: up is guaranteed >= (h,w)

        up = smooth(up, sigma=max(1.0, step / 2.5))

        field += amp * up
        amp_sum += amp
        amp *= persistence

    field /= max(amp_sum, 1e-9)
    lo, hi = np.percentile(field, [1, 99])
    field = np.clip((field - lo) / (hi - lo + 1e-9), 0, 1).astype(np.float32)
    return field


def write_geotiff(path, data, crs, transform, nodata=None, dtype=None, tags=None):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if data.ndim == 2:
        data = data[np.newaxis, ...]  # (bands, h, w)
    bands, h, w = data.shape
    if dtype is None:
        dtype = data.dtype

    profile = {
        "driver": "GTiff",
        "height": h,
        "width": w,
        "count": bands,
        "dtype": dtype,
        "crs": crs,
        "transform": transform,
        "compress": "deflate",
        "predictor": 2 if np.issubdtype(np.dtype(dtype), np.floating) else 1,
        "tiled": True,
        "blockxsize": max(16, (min(w, 256) // 16) * 16),
        "blockysize": max(16, (min(h, 256) // 16) * 16),
    }
    if nodata is not None:
        profile["nodata"] = nodata

    with rasterio.open(path, "w", **profile) as dst:
        for b in range(bands):
            dst.write(data[b].astype(dtype), b + 1)
        if tags:
            dst.update_tags(**tags)


def make_landcover_piecewise_constant(outdir, rng):
    """
    Categorical landcover (classes 1..6), with nodata holes and a nodata stripe.
    CRS: UTM zone 33N (EPSG:32633) somewhere near N Italy / Alps.
    """
    crs = CRS.from_epsg(32633)
    w, h = 512, 512
    px = 30.0  # 30 m
    # place upper-left roughly near (E=500000, N=5100000)
    transform = from_origin(500000.0, 5100000.0, px, px)

    f = nlm_fractal((h, w), rng, octaves=6, persistence=0.58)
    # skew distribution so classes occupy uneven areas
    g = np.clip(f**1.7, 0, 1)

    # quantize into 6 classes
    bins = np.quantile(g, [0.12, 0.28, 0.50, 0.72, 0.88])
    classes = np.digitize(g, bins).astype(np.uint8) + 1  # 1..6

    nodata = 0
    # nodata "lakes" (holes)
    holes = nlm_fractal((h, w), rng, octaves=4, persistence=0.65) > 0.92
    classes[holes] = nodata
    # nodata stripe (simulate missing swath)
    classes[:, 235:250] = nodata

    path = os.path.join(outdir, "landcover_utm33.tif")
    write_geotiff(
        path,
        classes,
        crs,
        transform,
        nodata=nodata,
        dtype="uint8",
        tags={
            "SEMANTICS": "piecewise_constant",
            "SUGGESTED_TRANSFER": "overlay_mode|overlay_weighted(fractions)|sample_nn",
        },
    )


def make_fractional_cover(outdir, rng):
    """
    Fractional tree cover 0..1 (float32), with nodata mask shaped like a coastline.
    CRS: same as landcover to allow comparison.
    """
    crs = CRS.from_epsg(32633)
    w, h = 512, 512
    px = 30.0
    transform = from_origin(500000.0, 5100000.0, px, px)

    base = nlm_fractal((h, w), rng, octaves=6, persistence=0.55)
    # produce patches with logistic contrast; keep continuous
    frac = 1.0 / (1.0 + np.exp(-8.0 * (base - 0.5)))
    frac = frac.astype(np.float32)

    # coastline-ish nodata mask: half-plane + noisy boundary
    yy, xx = np.mgrid[0:h, 0:w]
    boundary = (
        0.55 * w + 25 * (smooth(rng.random((h, w), dtype=np.float32), 6.0) - 0.5)
    ).astype(np.float32)
    mask = xx > boundary  # "ocean" nodata
    nodata = np.float32(-9999.0)
    frac = frac.copy()
    frac[mask] = nodata

    path = os.path.join(outdir, "frac_treecover_utm33.tif")
    write_geotiff(
        path,
        frac,
        crs,
        transform,
        nodata=nodata,
        dtype="float32",
        tags={
            "SEMANTICS": "fraction_cover",
            "SUGGESTED_TRANSFER": "overlay_weighted(mean)|histogram(numeric)|sample_interp(careful)",
        },
    )


def make_popcount_mass_in_cell(outdir, rng):
    """
    Extensive count raster: per-pixel totals, skewed heavy-tailed distribution.
    Includes a nodata polygon.
    CRS: Web Mercator (EPSG:3857) around Nairobi-ish (just for variety).
    """
    crs = CRS.from_epsg(3857)
    w, h = 600, 450
    px = 250.0  # 250 m
    # Upper-left: pick somewhere plausible in meters in 3857 (around East Africa region)
    # (This is synthetic; location just needs to be on Earth-ish.)
    transform = from_origin(4100000.0, -110000.0, px, px)

    field = nlm_fractal((h, w), rng, octaves=6, persistence=0.6)
    # Make a "city" hotspot + corridors
    yy, xx = np.mgrid[0:h, 0:w]
    cx, cy = int(0.62 * w), int(0.42 * h)
    r2 = (xx - cx) ** 2 + (yy - cy) ** 2
    hotspot = np.exp(-r2 / (2 * (0.12 * min(h, w)) ** 2)).astype(np.float32)
    corridor = np.exp(-((yy - 0.55 * h) ** 2) / (2 * (0.07 * h) ** 2)).astype(
        np.float32
    )

    intensity = np.clip(0.55 * field + 0.9 * hotspot + 0.3 * corridor, 0, 1)
    # Heavy-tailed counts: lognormal-ish
    counts = (np.exp(4.0 * intensity) - 1.0) * 3.0
    counts = np.rint(counts).astype(np.int32)

    nodata = -2147483648
    # nodata polygon: an arbitrary rotated rectangle mask
    x0, y0 = 0.2 * w, 0.2 * h
    x1, y1 = 0.55 * w, 0.35 * h
    theta = math.radians(25)
    X = (xx - x0) * math.cos(theta) + (yy - y0) * math.sin(theta)
    Y = -(xx - x0) * math.sin(theta) + (yy - y0) * math.cos(theta)
    poly_mask = (X > 0) & (X < (x1 - x0)) & (Y > 0) & (Y < (y1 - y0))
    counts = counts.copy()
    counts[poly_mask] = nodata

    path = os.path.join(outdir, "popcount_webmerc.tif")
    write_geotiff(
        path,
        counts,
        crs,
        transform,
        nodata=nodata,
        dtype="int32",
        tags={"SEMANTICS": "count_total", "SUGGESTED_TRANSFER": "mass_preserve(sum)"},
    )


def make_temp_cell_average_geographic(outdir, rng):
    """
    Continuous raster intended to be treated as cell-average (block support),
    stored in geographic CRS to exercise area-model choices.
    CRS: WGS84 (EPSG:4326) around SE Australia.
    """
    crs = CRS.from_epsg(4326)
    w, h = 720, 360
    # 0.01 degree pixels (~1.1 km lat, variable lon)
    res = 0.01
    # Upper-left lon/lat
    transform = from_origin(144.0, -36.0, res, res)  # around Melbourne-ish

    base = nlm_fractal((h, w), rng, octaves=6, persistence=0.55)
    grad = np.linspace(0, 1, w, dtype=np.float32)[None, :]
    temp = 12.0 + 10.0 * (0.65 * base + 0.35 * grad)  # ~12..22
    temp = temp.astype(np.float32)

    nodata = np.float32(-9999.0)
    temp = temp.copy()
    # nodata band near one edge + scattered missing pixels
    temp[:25, :] = nodata
    scatter = rng.random((h, w), dtype=np.float32) > 0.995
    temp[scatter] = nodata

    path = os.path.join(outdir, "temp_mean_wgs84.tif")
    write_geotiff(
        path,
        temp,
        crs,
        transform,
        nodata=nodata,
        dtype="float32",
        tags={
            "SEMANTICS": "cell_average",
            "SUGGESTED_TRANSFER": "overlay_weighted(mean, area-model matters)",
        },
    )


def make_zone_ids_laea(outdir, rng):
    """
    Categorical zone IDs in an equal-area CRS to test overlay behavior.
    CRS: Europe LAEA (EPSG:3035).
    """
    crs = CRS.from_epsg(3035)
    w, h = 500, 500
    px = 1000.0  # 1 km
    transform = from_origin(4000000.0, 3200000.0, px, px)

    yy, xx = np.mgrid[0:h, 0:w]
    # Build a few big zones as Voronoi-like regions from random seeds
    n_seeds = 8
    seeds = np.stack(
        [rng.integers(0, w, size=n_seeds), rng.integers(0, h, size=n_seeds)], axis=1
    )
    dmin = None
    idx = None
    for i, (sx, sy) in enumerate(seeds):
        d = (xx - sx) ** 2 + (yy - sy) ** 2
        if dmin is None:
            dmin = d
            idx = np.full((h, w), i, dtype=np.int32)
        else:
            mask = d < dmin
            idx[mask] = i
            dmin[mask] = d[mask]

    zones = (idx + 1).astype(np.uint16)  # 1..n
    nodata = 0
    # Add "islands" of nodata and tiny zones
    islands = nlm_fractal((h, w), rng, octaves=4, persistence=0.65) > 0.94
    zones[islands] = nodata
    # Tiny zone patches (simulate slivers)
    slivers = nlm_fractal((h, w), rng, octaves=5, persistence=0.5) > 0.985
    zones[slivers] = n_seeds + 1

    path = os.path.join(outdir, "zone_ids_laea.tif")
    write_geotiff(
        path,
        zones,
        crs,
        transform,
        nodata=nodata,
        dtype="uint16",
        tags={
            "SEMANTICS": "piecewise_constant",
            "SUGGESTED_TRANSFER": "overlay_mode|overlay_weighted(fractions)",
        },
    )


def make_multiband_per_band_nodata(outdir, rng):
    """
    4-band float32 raster where each band has nodata at *different* pixels.
    Specifically exercises the case where a pixel that is nodata in one band
    is valid in another, to verify that per-band nodata is handled correctly
    during DGGS indexing (step 1 pivot and step 2 aggregation).
    CRS: WGS84 (EPSG:4326), New Zealand region.
    """
    crs = CRS.from_epsg(4326)
    w, h = 120, 120
    res = 0.001  # ~100 m at this latitude
    transform = from_origin(174.0, -41.0, res, res)

    nodata = np.float32(-9999.0)
    bands = []
    for _ in range(4):
        field = nlm_fractal((h, w), rng, octaves=5, persistence=0.55)
        field = (10.0 + 90.0 * field).astype(np.float32)  # values in [10, 100]
        # Independent NLM mask per band so nodata locations differ across bands
        mask = nlm_fractal((h, w), rng, octaves=4, persistence=0.65) > 0.75
        field[mask] = nodata
        bands.append(field)

    data = np.stack(bands, axis=0)  # (4, h, w)
    path = os.path.join(outdir, "multiband_per_band_nodata_wgs84.tif")
    write_geotiff(
        path,
        data,
        crs,
        transform,
        nodata=nodata,
        dtype="float32",
        tags={
            "SEMANTICS": "multi_band_test",
            "NOTE": "per-band nodata at different pixels",
        },
    )


def make_satellite_swath(outdir, rng):
    """
    Simulated 3-band satellite swath in WGS84.

    The swath mask is computed using geodesic cross-track distance from a
    great-circle centreline, so the strip widens in longitude towards higher
    latitudes — matching the appearance of real polar-orbiting satellite swaths.
    Edge positions have gentle low-frequency noise to avoid perfectly straight
    margins.

    Coverage: 120°E–160°E, 20°N–60°N.  Pixel size: 0.05° (~5 km at equator).
    Swath half-width: 220 km.  Only ~15% of pixels contain valid data, making
    this a good stress-test for bbox-based DGGS cell enumeration.
    """
    import pyproj

    crs = CRS.from_epsg(4326)
    px = 0.05
    left, top_lat = 120.0, 60.0
    w, h = 800, 800  # 40° × 40°
    transform = from_origin(left, top_lat, px, px)

    # 3-band spatially autocorrelated data
    bands = [
        (nlm_fractal((h, w), rng, octaves=5, persistence=0.55) * 3000).astype(
            np.float32
        )
        for _ in range(3)
    ]
    data = np.stack(bands, axis=0)
    nodata = np.float32(-9999.0)

    # Pixel centre coordinates
    lons = left + (np.arange(w) + 0.5) * px
    lats = top_lat - (np.arange(h) + 0.5) * px
    lons_grid, lats_grid = np.meshgrid(lons, lats)

    # Swath centreline: great circle from SW corner to NE corner
    lon_A, lat_A = 120.0, 20.0
    lon_B, lat_B = 160.0, 60.0

    geod = pyproj.Geod(ellps="WGS84")
    az_AB, _, _ = geod.inv(lon_A, lat_A, lon_B, lat_B)

    # Azimuth and distance from centreline start to every pixel
    az_AP, _, dist_AP = geod.inv(
        np.full(lons_grid.shape, lon_A),
        np.full(lats_grid.shape, lat_A),
        lons_grid,
        lats_grid,
    )

    # Geodesic cross-track distance (metres); positive = right of centreline
    R = 6_371_000.0
    cross_track_m = (
        np.arcsin(
            np.clip(np.sin(dist_AP / R) * np.sin(np.radians(az_AP - az_AB)), -1.0, 1.0)
        )
        * R
    )

    # Swath half-width in metres + gentle along-track edge noise
    half_w_m = 220_000.0
    edge_noise_l = smooth(rng.random((h, w), dtype=np.float32), 20) * 40_000 - 20_000
    edge_noise_r = smooth(rng.random((h, w), dtype=np.float32), 20) * 40_000 - 20_000

    inside = (cross_track_m > -half_w_m + edge_noise_l) & (
        cross_track_m < half_w_m + edge_noise_r
    )
    data[:, ~inside] = nodata

    path = os.path.join(outdir, "swath_wgs84.tif")
    write_geotiff(
        path,
        data,
        crs,
        transform,
        nodata=nodata,
        dtype="float32",
        tags={
            "SEMANTICS": "point_center_strict",
            "NOTE": "Simulated 3-band satellite swath; rectangular in swath coords, curved in WGS84",
        },
    )


def make_u_shape_swath(outdir, rng):
    """
    Bounding-box stress test: a wide raster in Antarctic polar stereographic
    (EPSG:3031) positioned in the northern hemisphere.

    The raster is rectangular in the projected CRS, but its WGS84 footprint
    is a U-shape — the centre of the bottom edge dips ~10° further south than
    the four corners because constant-y lines in polar stereographic are arcs
    whose mid-point is closest to the opposite pole.

    Any algorithm that derives the WGS84 bounding box from only the four
    corner coordinates will report min_lat ≈ 20°N, missing real data at ~12°N.

    Raster: 20 000 km wide × 2 000 km tall, 10 km pixels (2000 × 200).
    Corners in WGS84: ~21–26°N.  Centre of bottom edge: ~11°N (~10° gap).
    """
    import pyproj

    crs = pyproj.CRS.from_epsg(3031)

    px = 10_000.0  # 10 km — coarse is fine; this is a geometry stress test
    w, h = 2_000, 200  # 20 000 km wide × 2 000 km tall
    cx, cy = 0, 16_000_000  # centre: 16 000 km from south pole, mid-latitudes N

    transform = from_origin(
        cx - w / 2 * px,  # left edge
        cy + h / 2 * px,  # top edge (northernmost y in EPSG:3031)
        px,
        px,
    )

    # Single-band continuous data
    data = (nlm_fractal((h, w), rng, octaves=4, persistence=0.55) * 3000).astype(
        np.float32
    )[np.newaxis, ...]

    path = os.path.join(outdir, "swath_u_shape.tif")
    write_geotiff(
        path,
        data,
        crs,
        transform,
        nodata=None,
        dtype="float32",
        tags={
            "SEMANTICS": "point_center_strict",
            "NOTE": (
                "U-shape bbox stress test: corners ~22-25°N, "
                "bottom edge centre ~12°N — naive corner bbox misses ~10° of data"
            ),
        },
    )


def make_satellite_swath_projected(outdir, rng):
    """
    Simulated 3-band satellite swath in Arctic polar stereographic CRS.

    The raster is a simple rectangle in the projected CRS, positioned off the
    central axis so it appears as a curved arc when raster2dggs transforms the
    pixel centroids to WGS84.  This exercises the CRS reprojection code path
    rather than the nodata-mask bbox stress test of swath_wgs84.tif.

    CRS: polar stereographic (North Pole, standard parallel 70°N, central
    meridian 145°E).  Strip is offset ~400 km east of the pole axis so it
    curves naturally across ~38°N–80°N, ~148°E–172°E in WGS84.
    Pixel size: 2 km.  Dimensions: 100 × 2 500 pixels (200 km × 5 000 km).
    """
    import pyproj

    # Use pyproj to construct the CRS so it generates proper WKT2 with a
    # named datum — rasterio's CRS.from_proj4 only embeds an ellipsoid,
    # which QGIS cannot resolve to a known CRS.
    crs = pyproj.CRS.from_dict(
        {
            "proj": "stere",
            "lat_0": 90,
            "lat_ts": 70,
            "lon_0": 145,
            "x_0": 0,
            "y_0": 0,
            "datum": "WGS84",
            "units": "m",
        }
    )

    px = 2_000.0
    w, h = 100, 2_500  # 200 km across-track × 5 000 km along-track

    # Offset 400 km east of the central meridian so the strip curves in WGS84
    # without crossing the antimeridian.
    # Upper-left corner is the near-pole end (less negative y = closer to pole).
    transform = from_origin(300_000.0, -1_000_000.0, px, px)

    # 3-band spatially autocorrelated data
    bands = [
        (nlm_fractal((h, w), rng, octaves=5, persistence=0.55) * 3000).astype(
            np.float32
        )
        for _ in range(3)
    ]
    data = np.stack(bands, axis=0)

    # Thin nodata margins at each swath edge (simulate sensor edge vignetting)
    nodata = np.float32(-9999.0)
    margin = 3
    data[:, :, :margin] = nodata
    data[:, :, -margin:] = nodata

    path = os.path.join(outdir, "swath_polar_stereo.tif")
    write_geotiff(
        path,
        data,
        crs,
        transform,
        nodata=nodata,
        dtype="float32",
        tags={
            "SEMANTICS": "point_center_strict",
            "NOTE": (
                "Polar stereographic swath; rectangle in CRS, "
                "curved arc in WGS84 (~40-75°N, ~150-175°E)"
            ),
        },
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--outdir", default="sample_rasters", help="Output directory")
    ap.add_argument("--seed", type=int, default=42, help="Random seed")
    args = ap.parse_args()

    rng = np.random.default_rng(args.seed)

    make_landcover_piecewise_constant(args.outdir, rng)
    make_fractional_cover(args.outdir, rng)
    make_popcount_mass_in_cell(args.outdir, rng)
    make_temp_cell_average_geographic(args.outdir, rng)
    make_zone_ids_laea(args.outdir, rng)
    make_multiband_per_band_nodata(args.outdir, rng)
    make_satellite_swath(args.outdir, rng)
    make_satellite_swath_projected(args.outdir, rng)
    make_u_shape_swath(args.outdir, rng)

    print(f"Wrote rasters to: {args.outdir}")
    print(
        f"scipy available: {HAVE_SCIPY} (gaussian smoothing {'enabled' if HAVE_SCIPY else 'using box blur fallback'})"
    )


if __name__ == "__main__":
    main()
