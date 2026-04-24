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
                acc += padded[dy:dy + out.shape[0], dx:dx + out.shape[1]]
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
    if os.path.exists(path):
        os.remove(path)
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
    g = np.clip(f ** 1.7, 0, 1)

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
        path, classes, crs, transform, nodata=nodata, dtype="uint8",
        tags={"SEMANTICS": "piecewise_constant", "SUGGESTED_TRANSFER": "overlay_mode|overlay_weighted(fractions)|sample_nn"}
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
    boundary = (0.55 * w + 25 * (smooth(rng.random((h, w), dtype=np.float32), 6.0) - 0.5)).astype(np.float32)
    mask = xx > boundary  # "ocean" nodata
    nodata = np.float32(-9999.0)
    frac = frac.copy()
    frac[mask] = nodata

    path = os.path.join(outdir, "frac_treecover_utm33.tif")
    write_geotiff(
        path, frac, crs, transform, nodata=nodata, dtype="float32",
        tags={"SEMANTICS": "fraction_cover", "SUGGESTED_TRANSFER": "overlay_weighted(mean)|histogram(numeric)|sample_interp(careful)"}
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
    corridor = np.exp(-((yy - 0.55 * h) ** 2) / (2 * (0.07 * h) ** 2)).astype(np.float32)

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
        path, counts, crs, transform, nodata=nodata, dtype="int32",
        tags={"SEMANTICS": "count_total", "SUGGESTED_TRANSFER": "mass_preserve(sum)"}
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
        path, temp, crs, transform, nodata=nodata, dtype="float32",
        tags={"SEMANTICS": "cell_average", "SUGGESTED_TRANSFER": "overlay_weighted(mean, area-model matters)"}
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
    seeds = np.stack([rng.integers(0, w, size=n_seeds), rng.integers(0, h, size=n_seeds)], axis=1)
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
    zones[slivers] = (n_seeds + 1)

    path = os.path.join(outdir, "zone_ids_laea.tif")
    write_geotiff(
        path, zones, crs, transform, nodata=nodata, dtype="uint16",
        tags={"SEMANTICS": "piecewise_constant", "SUGGESTED_TRANSFER": "overlay_mode|overlay_weighted(fractions)"}
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
        path, data, crs, transform, nodata=nodata, dtype="float32",
        tags={"SEMANTICS": "multi_band_test", "NOTE": "per-band nodata at different pixels"},
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

    print(f"Wrote rasters to: {args.outdir}")
    print(f"scipy available: {HAVE_SCIPY} (gaussian smoothing {'enabled' if HAVE_SCIPY else 'using box blur fallback'})")


if __name__ == "__main__":
    main()
