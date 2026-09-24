"""Regenerate the README figures.

Self-contained: fetches the source rasters from their public homes, indexes
them with the raster2dggs CLI, and renders the figures into this directory.
Needs raster2dggs[all] plus matplotlib and the gdal_translate binary, network
access, and a few minutes of CPU.

    python docs/imgs/generate_figures.py [--workdir DIR]

Sources (all CC BY 4.0):
- LINZ 0.3 m aerial imagery, Manawatū-Whanganui 2021-2022, tile BM35 — the
  hero imagery (RGB + NIR + alpha).
- LINZ 10 m national satellite mosaic 2024-2025, tile AS24 — the only tile in
  that mosaic whose extent crosses the mosaic's coverage boundary, so its
  alpha band demonstrates mask handling in the sampling figure.
- ESA WorldCover 10 m 2021 v200, windowed over Rangatira (South East Island,
  Chatham Islands) — a categorical raster for the compaction figure, and the
  same island as vector2dggs's README figures.

The struct figure is the exception: it needs a continuous field with known
units and a controlled spread, so it synthesises one with the repo's own
neutral landscape model (make_samples.nlm_fractal) rather than fetching it.
"""

import argparse
import os
import shutil
import subprocess
import sys
import tempfile

import matplotlib

matplotlib.use("Agg")
import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import pyarrow.parquet as pq
import rasterio
import shapely
from matplotlib.collections import PolyCollection
from matplotlib.patches import Patch, PathPatch
from matplotlib.path import Path

HERE = os.path.dirname(os.path.abspath(__file__))

NZ_IMAGERY = "/vsicurl/https://nz-imagery.s3.ap-southeast-2.amazonaws.com"
BM35 = f"{NZ_IMAGERY}/manawatu-whanganui/manawatu-whanganui_2021-2022_0.3m/rgbnir/2193/BM35_10000_0402.tiff"
AS24 = f"{NZ_IMAGERY}/new-zealand/new-zealand_2024-2025_10m/rgb/2193/AS24.tiff"
WORLDCOVER = (
    "/vsis3/esa-worldcover/v200/2021/map/ESA_WorldCover_10m_2021_v200_S45W177_Map.tif"
)

INK, MUT, FAINT, NOTE = "#1a1a1a", "#555555", "#888888", "#b0332a"
MONO = "DejaVu Sans Mono"
# Neutral for glyph marks whose length encodes a count rather than a raster
# value, so the YlGn ramp only ever means "this is a value".
GLYPH = "#9aa2ab"
MASKC = "#ebebeb"
# ESA WorldCover class palette (their standard colours); identity is also
# carried by the legend, and the three classes separate in lightness.
WC_CLASSES = {
    10: ("Tree cover", "#006400"),
    30: ("Grassland", "#ffff4c"),
    80: ("Open water", "#0064c8"),
}
# Struct figure: one neutral landscape model written as two co-registered
# views, so the continuous and classified panels show the same landscape.
# 50 m pixels put ~43 pixel centres in an H3 r9 cell (~0.105 km²), enough for a
# distribution while still letting individual pixels read at panel scale.
# A5 rather than H3: its cells are equal-area, so --overlay's area weights and
# --overlay fractions mean exactly what they say. r14 puts ~46 pixel centres in
# a cell; r18 is four levels finer, so 256 of them tile one r14 cell.
NLM_SEED, NLM_PX, NLM_SIZE = 7, 50.0, 160
STRUCT_DGGS, STRUCT_RES, SAMPLE_RES = "a5", 14, 18
CLASS_STEPS = [0.22, 0.45, 0.68, 0.92]
CLASS_NAME = {1: "bare", 2: "grass", 3: "shrub", 4: "forest"}
HERO_RES = (12, 13)  # H3 resolutions for the hero's map panels
# DGGS and resolutions for the "one argument picks the grid" strip: chosen so
# cells are a legible size at strip-panel scale (~25-40 m over a 307 m window).
STRIP = [("h3", 12), ("rhp", 13), ("a5", 19), ("s2", 18), ("geohash", 8)]

plt.rcParams.update(
    {
        "font.family": "DejaVu Sans",
        "text.color": INK,
        "axes.edgecolor": "#bbbbbb",
        "figure.facecolor": "white",
    }
)


def sh(*args, env_extra=None):
    env = dict(os.environ, **(env_extra or {}))
    subprocess.run(args, check=True, env=env, stdout=subprocess.DEVNULL)


def fetch(src, dst, *window):
    if not os.path.exists(dst):
        sh(
            "gdal_translate",
            "-q",
            "-co",
            "COMPRESS=DEFLATE",
            "-co",
            "TILED=YES",
            *window,
            src,
            dst,
            env_extra={
                "GDAL_DISABLE_READDIR_ON_OPEN": "EMPTY_DIR",
                "AWS_NO_SIGN_REQUEST": "YES",
            },
        )


def r2d(dggs, src, out, res, *extra):
    sh(
        sys.executable,
        "-c",
        "from raster2dggs.cli import main; main()",
        dggs,
        str(src),
        str(out),
        "-r",
        str(res),
        "-p",
        "8",
        "-o",
        *extra,
    )


def acquire(w):
    """Fetch the source windows (idempotent)."""
    fetch(BM35, f"{w}/hero.tif", "-srcwin", "4000", "3000", "4096", "4096")
    fetch(BM35, f"{w}/hero_strip.tif", "-srcwin", "5200", "4600", "1024", "1024")
    fetch(AS24, f"{w}/as24_edge.tif", "-srcwin", "1152", "0", "512", "512")
    fetch(
        WORLDCOVER,
        f"{w}/worldcover.tif",
        "-projwin",
        "-176.196",
        "-44.329",
        "-176.150",
        "-44.363",
    )


def synth(w):
    """Write one NLM field as a continuous raster and its quantile classification."""
    cont, cat = f"{w}/cover.tif", f"{w}/cover_class.tif"
    if os.path.exists(cont) and os.path.exists(cat):
        return cont, cat
    sys.path.insert(0, os.path.dirname(os.path.dirname(HERE)))
    from make_samples import nlm_fractal

    rng = np.random.default_rng(NLM_SEED)
    n = NLM_SIZE
    base = nlm_fractal((n, n), rng, octaves=6, persistence=0.55)
    # Logistic contrast, as make_fractional_cover does: pushes values toward the
    # ends, so a binned histogram of one cell has shape rather than one spike.
    cover = (1.0 / (1.0 + np.exp(-8.0 * (base - 0.5)))).astype(np.float32)
    classes = (np.digitize(cover, np.quantile(cover, [0.25, 0.55, 0.80])) + 1).astype(
        np.uint8
    )
    profile = dict(
        driver="GTiff",
        height=n,
        width=n,
        count=1,
        crs=rasterio.crs.CRS.from_epsg(2193),
        transform=rasterio.transform.from_origin(
            1_750_000.0, 5_600_000.0, NLM_PX, NLM_PX
        ),
        compress="deflate",
    )
    for path, arr, dtype, nodata in [
        (cont, cover, "float32", None),
        (cat, classes, "uint8", 0),
    ]:
        with rasterio.open(path, "w", dtype=dtype, nodata=nodata, **profile) as d:
            d.write(arr, 1)
    return cont, cat


def index_all(w):
    hero, strip_src = f"{w}/hero.tif", f"{w}/hero_strip.tif"
    edge, wc = f"{w}/as24_edge.tif", f"{w}/worldcover.tif"
    cover, cover_cls = f"{w}/cover.tif", f"{w}/cover_class.tif"
    sr, poly = STRUCT_RES, ("-g", "polygon")
    rgb = ("-b", "1", "-b", "2", "-b", "3", "-d", "0")
    r_lo, r_hi = HERO_RES
    jobs = {
        f"hero_h3_{r_lo}": lambda o: r2d("h3", hero, o, r_lo, *rgb, "-g", "polygon"),
        f"hero_h3_{r_hi}": lambda o: r2d(
            "h3", hero, o, r_hi, *rgb, "-g", "polygon", "-c", "brotli"
        ),
        "disk_string": lambda o: r2d("h3", hero, o, r_hi, *rgb, "-c", "brotli"),
        "disk_u64": lambda o: r2d(
            "h3", hero, o, r_hi, *rgb, "--cell-id", "uint64", "-c", "brotli"
        ),
        "disk_geo_point": lambda o: r2d(
            "h3", hero, o, r_hi, *rgb, "-g", "point", "-c", "brotli"
        ),
        "samp_point": lambda o: r2d(
            "h3", edge, o, 12, *rgb, "--point", "value", "-g", "polygon"
        ),
        "samp_overlay": lambda o: r2d(
            "h3", edge, o, 12, *rgb, "--overlay", "weighted", "-g", "polygon"
        ),
        "samp_bilinear": lambda o: r2d(
            "h3", edge, o, 12, *rgb, "--sample", "bilinear", "-g", "polygon"
        ),
        "comp": lambda o: r2d(
            "rhp", wc, o, 12, "-d", "0", "--overlay", "mode", "-g", "polygon"
        ),
        "comp_co": lambda o: r2d(
            "rhp", wc, o, 12, "-d", "0", "--overlay", "mode", "-co", "-g", "polygon"
        ),
        # The struct figure. --overlay fractions needs -d 3: --decimals rounds
        # the fractions too, and -d 0 would flatten them to 0/1.
        "struct_agg": lambda o: r2d(
            STRUCT_DGGS,
            cover,
            o,
            sr,
            "-d",
            "3",
            "--point",
            "value",
            "-a",
            "min,max,mean",
            *poly,
        ),
        "struct_list": lambda o: r2d(
            STRUCT_DGGS, cover, o, sr, "-d", "3", "--point", "list", *poly
        ),
        "struct_hist": lambda o: r2d(
            STRUCT_DGGS,
            cover,
            o,
            sr,
            "-d",
            "3",
            "--point",
            "histogram",
            "--hist-width",
            "0.1",
            *poly,
        ),
        "struct_ovhist": lambda o: r2d(
            STRUCT_DGGS,
            cover,
            o,
            sr,
            "-d",
            "3",
            "--overlay",
            "histogram",
            "--hist-width",
            "0.1",
            *poly,
        ),
        "struct_frac": lambda o: r2d(
            STRUCT_DGGS, cover_cls, o, sr, "-d", "3", "--overlay", "fractions", *poly
        ),
        # No --hist-width: on a categorical raster histogram counts exact values,
        # giving one bin per class and the struct<values, counts> schema.
        "struct_cathist": lambda o: r2d(
            STRUCT_DGGS, cover_cls, o, sr, "-d", "0", "--overlay", "histogram", *poly
        ),
        # -d 0 on the same fractions, for the --decimals footnote
        "struct_frac_d0": lambda o: r2d(
            STRUCT_DGGS, cover_cls, o, sr, "-d", "0", "--overlay", "fractions", *poly
        ),
        # Oversampled: cells finer than the pixels, where interpolation shows.
        # Run on both rasters — on the classified one bilinear is the wrong
        # choice, and the figure says so.
        **{
            f"struct_{k}samp_{m}": (
                lambda o, m=m, src=src: r2d(
                    STRUCT_DGGS, src, o, SAMPLE_RES, "-d", "3", "--sample", m, *poly
                )
            )
            for k, src in (("", cover), ("cat", cover_cls))
            for m in ("nn", "bilinear")
        },
        "struct_mode": lambda o: r2d(
            STRUCT_DGGS, cover_cls, o, sr, "-d", "0", "--overlay", "mode", *poly
        ),
        **{
            f"strip_{d}": (
                lambda o, d=d, r=r: r2d(d, strip_src, o, r, *rgb, "-g", "polygon")
            )
            for d, r in STRIP
        },
    }
    for name, job in jobs.items():
        out = f"{w}/runs/{name}"
        if not os.path.exists(out):
            print(f"  indexing {name}")
            job(out)


class Style:
    """Display stretch for one source raster, shared by its panels."""

    def __init__(self, tif):
        with rasterio.open(tif) as s:
            self.rgb_raw = np.transpose(s.read([1, 2, 3]), (1, 2, 0)).astype(float)
            self.alpha = s.dataset_mask()
            self.bounds = s.bounds
        self.lo, self.hi = np.percentile(
            self.rgb_raw[self.alpha == 255], [2, 98], axis=0
        )

    def stretch(self, a):
        d = np.clip((a - self.lo) / (self.hi - self.lo), 0, 1) ** 0.9
        hsv = matplotlib.colors.rgb_to_hsv(d)
        hsv[..., 1] = np.clip(hsv[..., 1] * 1.25, 0, 1)
        return matplotlib.colors.hsv_to_rgb(hsv)

    def display_image(self):
        d = self.stretch(self.rgb_raw)
        d[self.alpha == 0] = np.array(matplotlib.colors.to_rgb(MASKC))
        return d


def cells(w, run, crs="EPSG:2193"):
    df = pq.read_table(f"{w}/runs/{run}").to_pandas()
    g = gpd.GeoDataFrame(
        df.drop(columns=["geometry"]),
        geometry=gpd.GeoSeries(
            shapely.from_wkb(df["geometry"]), index=df.index, crs=4326
        ),
    )
    return g.to_crs(crs) if crs else g


def plot_cells(ax, g, colors, lw=0.0, ec="none"):
    verts = [np.asarray(p.exterior.coords) for p in g.geometry]
    ax.add_collection(
        PolyCollection(
            verts, facecolors=colors, edgecolors=ec, linewidths=lw, rasterized=True
        )
    )


def band_colors(style, g):
    return style.stretch(np.c_[g["band_1"], g["band_2"], g["band_3"]])


def dir_kb(d):
    return round(
        sum(os.path.getsize(os.path.join(r, f)) for r, _, fs in os.walk(d) for f in fs)
        / 1024
    )


def fig_hero(w, style):
    b = style.bounds
    r_lo, r_hi = HERO_RES
    fig = plt.figure(figsize=(13.4, 13.4), dpi=100)
    fig.text(
        0.018,
        0.980,
        "raster2dggs — raster in, DGGS cells out",
        size=21,
        weight="bold",
        va="top",
    )
    fig.text(
        0.018,
        0.951,
        "LINZ 0.3 m aerial imagery indexed to H3  ·  bands become columns, one row per cell",
        size=12.5,
        color=MUT,
        va="top",
    )
    fig.text(
        0.018,
        0.917,
        "Every pixel lands in the DGGS cell containing its centre, and each cell aggregates its pixels' band values\n"
        "A finer resolution keeps more of the image — and costs proportionally more rows",
        size=11,
        va="top",
        linespacing=1.5,
    )

    n13 = 0
    for x, (title, run) in zip(
        (0.018, 0.352, 0.686),
        [
            ("Raster input", None),
            (f"H3 resolution {r_lo}", f"hero_h3_{r_lo}"),
            (f"H3 resolution {r_hi}", f"hero_h3_{r_hi}"),
        ],
        strict=True,
    ):
        ax = fig.add_axes([x, 0.555, 0.30, 0.315])
        ax.set_title(title, size=13, pad=6)
        ax.set_xticks([]), ax.set_yticks([])
        if run is None:
            ax.imshow(style.display_image(), extent=(b.left, b.right, b.bottom, b.top))
            with rasterio.open(f"{w}/hero.tif") as s:
                interp = "·".join(c.name for c in s.colorinterp)
                badge = f"{s.width:,} × {s.height:,} px · {s.count} bands ({interp})"
        else:
            g = cells(w, run)
            plot_cells(ax, g, band_colors(style, g))
            if run.endswith(str(r_hi)):
                n13 = len(g)
            badge = f"{len(g):,} cells"
        ax.set_xlim(b.left, b.right), ax.set_ylim(b.bottom, b.top)
        ax.set_aspect("equal")
        ax.text(
            0.03,
            0.03,
            badge,
            transform=ax.transAxes,
            size=10,
            bbox=dict(boxstyle="round,pad=0.35", fc="white", ec="#cccccc"),
        )
    fig.text(
        0.352,
        0.532,
        "-b 1 -b 2 -b 3 selects the visible bands of the 5-band source; any subset of bands, by index or\n"
        "name, rides along as columns · declared nodata and alpha/dataset masks are honoured throughout",
        size=10.5,
        color=MUT,
        va="top",
        linespacing=1.45,
    )

    fig.text(
        0.018,
        0.478,
        "One argument picks the grid — the rest of the command is unchanged",
        size=12,
        weight="bold",
        va="top",
    )
    with rasterio.open(f"{w}/hero_strip.tif") as src:
        cb = src.bounds
    strip_style = Style(f"{w}/hero_strip.tif")
    names = {
        "h3": "H3",
        "rhp": "rHEALPix",
        "a5": "A5",
        "s2": "S2",
        "geohash": "Geohash",
    }
    for i, (d, r) in enumerate(STRIP):
        ax = fig.add_axes([0.018 + i * 0.1965, 0.288, 0.185, 0.150])
        g = cells(w, f"strip_{d}")
        plot_cells(ax, g, band_colors(strip_style, g))
        ax.set_xlim(cb.left, cb.right), ax.set_ylim(cb.bottom, cb.top)
        ax.set_aspect("equal"), ax.set_xticks([]), ax.set_yticks([])
        ax.set_title(f"{names[d]} · r{r}", size=10.5, pad=4)
    fig.text(
        0.018,
        0.272,
        "raster2dggs  h3 | rhp | a5 | s2 | geohash | maidenhead | 16 DGGAL grids   input.tif   out/",
        size=10,
        family="DejaVu Sans Mono",
        color=MUT,
        va="top",
    )

    fig.text(
        0.018,
        0.228,
        "Pixels in  ·  one row per cell out",
        size=12,
        weight="bold",
        va="top",
    )
    t = pq.read_table(f"{w}/runs/disk_string").to_pandas().reset_index()
    cell_cols = [c for c in t.columns if c.startswith("h3")]
    idx_col, parent_col = cell_cols[0], cell_cols[1]
    lines = [f"{idx_col:17s}{parent_col:17s}{'band_1':>7s}{'band_2':>7s}{'band_3':>7s}"]
    for _, r in t.head(4).iterrows():
        lines.append(
            f"{r[idx_col]:17s}{r[parent_col]:17s}{int(r['band_1']):7d}{int(r['band_2']):7d}{int(r['band_3']):7d}"
        )
    lines.append(f"… {len(t) - 4:,} more")
    fig.text(
        0.018,
        0.198,
        "\n".join(lines),
        size=9.5,
        family="DejaVu Sans Mono",
        va="top",
        linespacing=1.6,
    )
    fig.text(
        0.018,
        0.078,
        "-a mean,std,min …   adds a struct column per band\n"
        "--cell-id uint64    native integer cell IDs (H3, S2, A5, DGGAL)\n"
        "-g polygon | point  GeoParquet cell geometry",
        size=9.5,
        family="DejaVu Sans Mono",
        color=MUT,
        va="top",
        linespacing=1.7,
    )

    fig.text(
        0.52,
        0.228,
        "On disk  ·  hive-partitioned by parent cell  ·  -c brotli",
        size=12,
        weight="bold",
        va="top",
    )
    parts = sorted(p for p in os.listdir(f"{w}/runs/disk_string") if "=" in p)
    tree = (
        ["out/"]
        + [f"  {p}/part.0.parquet" for p in parts[:2]]
        + [f"  … {len(parts)} partitions in all"]
    )
    fig.text(
        0.52,
        0.198,
        "\n".join(tree),
        size=9.5,
        family="DejaVu Sans Mono",
        va="top",
        linespacing=1.6,
    )
    with rasterio.open(f"{w}/hero.tif") as s:
        px_per_cell = (s.width * s.height) / n13
    in_mb = os.path.getsize(f"{w}/hero.tif") / 1024**2
    fig.text(
        0.52,
        0.130,
        f"A reader can skip whole partitions without opening them\n"
        f"{n13:,} rows at H3 res {r_hi} · {in_mb:.0f} MB source window · ≈ {px_per_cell:,.0f} pixels per cell",
        size=10,
        color=MUT,
        va="top",
        linespacing=1.5,
    )

    kb = {
        "string": dir_kb(f"{w}/runs/disk_string"),
        "u64": dir_kb(f"{w}/runs/disk_u64"),
        "geo": dir_kb(f"{w}/runs/hero_h3_{r_hi}"),
        "geo_point": dir_kb(f"{w}/runs/disk_geo_point"),
    }
    bars = [
        ("Parquet, string cell IDs", kb["string"], "#2f6fb7"),
        ("Parquet, --cell-id uint64", kb["u64"], "#2f6fb7"),
        ("GeoParquet, -g point", kb["geo_point"], "#d5c8a6"),
        ("GeoParquet, -g polygon", kb["geo"], "#c3b091"),
    ]
    bx = fig.add_axes([0.665, 0.012, 0.30, 0.082])
    xmax = max(v for _, v, _ in bars) * 1.22
    for y, (label, v, c) in zip(np.arange(len(bars))[::-1], bars, strict=True):
        bx.barh(y, v, height=0.62, color=c)
        bx.text(v + xmax * 0.017, y, f"{v:,} KB", va="center", size=9.5)
        bx.text(-xmax * 0.017, y, label, va="center", ha="right", size=9.5)
    bx.set_xlim(0, xmax), bx.set_ylim(-0.6, len(bars) - 0.4), bx.axis("off")

    fig.text(
        0.982,
        0.006,
        "raster2dggs · LINZ 0.3 m aerial imagery, Manawatū-Whanganui 2021–2022 (CC BY 4.0) · tile BM35 crop",
        size=9,
        color=FAINT,
        ha="right",
    )
    fig.savefig(f"{HERE}/raster2dggs-example.png", dpi=100)
    plt.close(fig)


def fig_sampling(w, style):
    b = style.bounds
    # zoom straddling the mask boundary: centred on the boundary's mean row
    first_valid = np.argmax(style.alpha == 255, axis=0)
    c0, c1 = 140, 220
    mid = int(first_valid[c0:c1].mean())
    r0, r1 = mid - 40, mid + 40
    zx0, zx1 = b.left + c0 * 10, b.left + c1 * 10
    zy1, zy0 = b.top - r0 * 10, b.top - r1 * 10

    fig = plt.figure(figsize=(13.4, 4.9), dpi=100)
    fig.text(
        0.018,
        0.965,
        "Three ways to move pixel values onto cells",
        size=15,
        weight="bold",
        va="top",
    )
    fig.text(
        0.018,
        0.885,
        "The choice matters most at edges — here, a mask boundary in the LINZ 10 m mosaic "
        "(zoomed; H3 resolution 12, cells ≈ 2 × 2 pixels)",
        size=11,
        color=MUT,
        va="top",
    )
    panels = [
        (None, "Input (zoom)", "Grey = masked (alpha 0)"),
        (
            "samp_point",
            "--point value",
            "Cell centre's pixel value\nFast, exact at native scale",
        ),
        (
            "samp_overlay",
            "--overlay weighted",
            "Area-weighted mean of every\nvalid pixel the cell touches",
        ),
        (
            "samp_bilinear",
            "--sample bilinear",
            "Kernel interpolation at the\ncell centre (bicubic, lanczos…)",
        ),
    ]
    for i, (run, title, caption) in enumerate(panels):
        ax = fig.add_axes([0.018 + i * 0.247, 0.17, 0.232, 0.62])
        if run is None:
            ax.imshow(style.display_image(), extent=(b.left, b.right, b.bottom, b.top))
        else:
            g = cells(w, run)
            plot_cells(ax, g, band_colors(style, g))
            ax.text(
                0.03,
                0.03,
                f"{len(g):,} cells (whole raster)",
                transform=ax.transAxes,
                size=9,
                bbox=dict(boxstyle="round,pad=0.3", fc="white", ec="#cccccc"),
            )
        ax.set_xlim(zx0, zx1), ax.set_ylim(zy0, zy1)
        ax.set_aspect("equal"), ax.set_xticks([]), ax.set_yticks([])
        ax.set_title(
            title, size=12, family="DejaVu Sans Mono" if run else "DejaVu Sans", pad=5
        )
        ax.text(
            0.5,
            -0.09,
            caption,
            transform=ax.transAxes,
            ha="center",
            va="top",
            size=9.5,
            color=MUT,
            linespacing=1.4,
        )
    fig.text(
        0.982,
        0.02,
        "raster2dggs · LINZ 10 m national satellite mosaic 2024–2025 (CC BY 4.0) · tile AS24, at the mosaic's coverage edge",
        size=8.5,
        color=FAINT,
        ha="right",
    )
    fig.savefig(f"{HERE}/sampling-strategies.png", dpi=100)
    plt.close(fig)


def fig_compaction(w):
    fig = plt.figure(figsize=(13.4, 6.3), dpi=100)
    fig.text(
        0.018,
        0.972,
        "--compact merges complete groups of siblings with identical values",
        size=15,
        weight="bold",
        va="top",
    )
    fig.text(
        0.018,
        0.910,
        "ESA WorldCover 10 m land cover around Rangatira (South East Island), indexed to rHEALPix "
        "resolution 12 with --overlay mode (majority class per cell)",
        size=11,
        color=MUT,
        va="top",
    )
    counts, xs = {}, None
    for i, (run, title) in enumerate(
        [
            ("comp", "As indexed · resolution 12"),
            ("comp_co", "With --compact · mixed resolutions"),
        ]
    ):
        g = cells(w, run, crs=None)
        counts[run] = len(g)
        ax = fig.add_axes([0.018 + i * 0.505, 0.10, 0.435, 0.72])
        colors = [WC_CLASSES[int(v)][1] for v in g["band_1"]]
        lw = 0.0 if run == "comp" else 0.25
        plot_cells(ax, g, colors, lw=lw, ec="white" if lw else "none")
        if xs is None:
            xs = g.total_bounds
        ax.set_xlim(xs[0], xs[2]), ax.set_ylim(xs[1], xs[3])
        ax.set_aspect(1 / np.cos(np.radians((xs[1] + xs[3]) / 2)))
        ax.set_xticks([]), ax.set_yticks([])
        ax.set_title(title, size=12.5, pad=6)
        ax.text(
            0.03,
            0.03,
            f"{len(g):,} rows",
            transform=ax.transAxes,
            size=10.5,
            bbox=dict(boxstyle="round,pad=0.35", fc="white", ec="#cccccc"),
        )
    fig.text(
        0.494,
        0.46,
        f"{counts['comp'] / counts['comp_co']:.1f}×\nfewer\nrows",
        size=13,
        weight="bold",
        ha="center",
        va="center",
    )
    fig.legend(
        handles=[
            Patch(fc=c, label=n) for n, c in (WC_CLASSES[k] for k in (10, 30, 80))
        ],
        loc="lower left",
        bbox_to_anchor=(0.017, 0.012),
        ncol=3,
        frameon=False,
        fontsize=10.5,
    )
    fig.text(
        0.982,
        0.022,
        "raster2dggs · ESA WorldCover 2021 v200 (CC BY 4.0)",
        size=8.5,
        color=FAINT,
        ha="right",
    )
    fig.savefig(f"{HERE}/compaction-example.png", dpi=100)
    plt.close(fig)


def pick_struct_cell(w):
    """The cell the struct figure explodes: widest spread over the most classes.

    Chosen rather than hardcoded so the figure survives a change of seed or
    grid; deterministic, since the landscape model is seeded.
    """
    lst = pq.read_table(f"{w}/runs/struct_list").to_pandas()
    frac = pq.read_table(f"{w}/runs/struct_frac").to_pandas()
    hist = pq.read_table(f"{w}/runs/struct_hist").to_pandas()
    best, best_score = None, -1.0
    for cid in lst.index.intersection(frac.index).intersection(hist.index):
        v = np.asarray(lst.loc[cid, "band_1"], dtype=float)
        if len(v) < 35:
            continue
        score = (
            v.std()
            * (v.max() - v.min())
            * len(frac.loc[cid, "band_1"]["classes"])
            * int((np.asarray(hist.loc[cid, "band_1"]["counts"]) > 0).sum())
        )
        if score > best_score:
            best, best_score = cid, score
    return best


def fig_structs(w):
    """One cell, every output schema.

    Blocked by *input raster* rather than by --point/--overlay, so each block
    lines up with the panel on the left that it reads from; the route stays
    legible because every row is labelled with the flag that produced it.

    Colour is load-bearing: the YlGn ramp appears only where a mark carries a
    raster value or a class identity. Marks whose length encodes a count are
    neutral, so green never reads as decoration.
    """
    cover = plt.get_cmap("YlGn")
    cid = pick_struct_cell(w)

    def band(run):
        return pq.read_table(f"{w}/runs/{run}").to_pandas().loc[cid, "band_1"]

    with rasterio.open(f"{w}/cover.tif") as s:
        cont, bounds = s.read(1), s.bounds
    with rasterio.open(f"{w}/cover_class.tif") as s:
        classified = s.read(1)

    g = cells(w, "struct_list")
    demo = g.loc[cid, "geometry"]
    cx, cy = demo.centroid.x, demo.centroid.y
    half, area_km2 = 400.0, demo.area / 1e6
    lst = np.asarray(band("struct_list"), dtype=float)
    agg = band("struct_agg")
    classes = [int(c) for c in band("struct_frac")["classes"]]
    fracs = [float(f) for f in band("struct_frac")["fractions"]]
    fracs_d0 = [float(f) for f in band("struct_frac_d0")["fractions"]]
    mode = int(band("struct_mode"))
    cat_h = band("struct_cathist")
    cat_vals = [int(v) for v in cat_h["values"]]
    cat_counts = [int(c) for c in cat_h["counts"]]
    n_pt = int(np.asarray(band("struct_hist")["counts"]).sum())
    n_ov = int(np.asarray(band("struct_ovhist")["counts"]).sum())

    # The oversampled cells lying in this one, for the two --sample rows.
    # Centre-containment rather than the ID hierarchy: A5's nesting is
    # non-congruent, so a cell's 256 true descendants do not tile its geometry
    # (the two sets disagree both ways on roughly a third of the cells here).
    # What the panel is illustrating is the sampled surface over this cell's
    # footprint, so the footprint is what should select it.
    def inside(run):
        sub = cells(w, run)
        minx, miny, maxx, maxy = demo.bounds
        sub = sub.cx[minx:maxx, miny:maxy]
        return sub[sub.geometry.centroid.within(demo)]

    samp = {
        f"{k}{m}": inside(f"struct_{k}samp_{m}")
        for k in ("", "cat")
        for m in ("nn", "bilinear")
    }
    n_samp = len(samp["bilinear"])
    n_cat_distinct = samp["catbilinear"]["band_1"].nunique()

    fig = plt.figure(figsize=(13.4, 12.0), dpi=100)
    fig.text(
        0.018,
        0.986,
        "What a cell holds when many pixels fall inside",
        size=15,
        weight="bold",
        va="top",
    )
    fig.text(
        0.018,
        0.957,
        f"One A5 resolution {STRUCT_RES} cell over a synthetic neutral landscape model "
        f"at {NLM_PX:.0f} m \u2014 {n_pt} pixel centres inside it, {n_ov} pixels touched.\n"
        "The column type is the choice: a scalar discards the distribution, a struct "
        "keeps it. Every value below is real CLI output for the outlined cell.",
        size=10.5,
        color=MUT,
        va="top",
        linespacing=1.5,
    )

    # ---- left: one panel per input, each aligned with its block ----------
    extent = (bounds.left, bounds.right, bounds.bottom, bounds.top)
    panels = [
        (
            cont,
            0.645,
            "Continuous \u2014 tree cover 0\u20131",
            dict(cmap=cover, vmin=0, vmax=1),
        ),
        (
            classified,
            0.272,
            "Classified \u2014 land cover classes",
            dict(
                cmap=matplotlib.colors.ListedColormap([cover(s) for s in CLASS_STEPS]),
                vmin=0.5,
                vmax=4.5,
            ),
        ),
    ]
    for arr, y, title, kw in panels:
        fig.text(0.018, y + 0.228, title, size=9.5, color=MUT, va="bottom")
        ax = fig.add_axes([0.018, y, 0.196, 0.220])
        ax.imshow(arr, extent=extent, interpolation="nearest", **kw)
        # Mid grey, semi-transparent: it darkens the pale end of the ramp and
        # lightens the dark end, so one stroke reads across the whole surface
        # where white only held over the dark greens.
        ax.add_collection(
            PolyCollection(
                [np.asarray(p.exterior.coords) for p in g.geometry],
                facecolors="none",
                edgecolors=FAINT,
                linewidths=0.9,
                alpha=0.8,
            )
        )
        ax.add_collection(
            PolyCollection(
                [np.asarray(demo.exterior.coords)],
                facecolors="none",
                edgecolors=NOTE,
                linewidths=2.4,
            )
        )
        ax.set_xlim(cx - half, cx + half), ax.set_ylim(cy - half, cy + half)
        ax.set_aspect("equal"), ax.set_xticks([]), ax.set_yticks([])

    # class identity by name, never by colour alone
    lg = fig.add_axes([0.018, 0.222, 0.196, 0.026])
    for i, c in enumerate(CLASS_NAME):
        lg.add_patch(
            plt.Rectangle(
                (i * 0.25, 0.45), 0.055, 0.42, color=cover(CLASS_STEPS[c - 1]), lw=0
            )
        )
        lg.text(i * 0.25 + 0.072, 0.66, CLASS_NAME[c], size=8, color=MUT, va="center")
    lg.set_xlim(0, 1), lg.set_ylim(0, 1)
    lg.set_axis_off()
    # names the outlined cell, keyed by a sample of the outline itself
    key = fig.add_axes([0.018, 0.186, 0.196, 0.022])
    key.plot([0.0, 0.075], [0.5, 0.5], color=NOTE, lw=2.4)
    key.text(0.10, 0.5, cid, size=8.5, family=MONO, color=MUT, va="center")
    key.set_xlim(0, 1), key.set_ylim(0, 1)
    key.set_axis_off()
    fig.text(
        0.018,
        0.168,
        f"Every r{STRUCT_RES} cell is {area_km2:.3f} km\u00b2 \u2248 {n_pt} pixels at "
        f"{NLM_PX:.0f} m.",
        size=9,
        color=MUT,
        va="top",
    )

    # ---- right: one row per output schema --------------------------------
    X_FLAG, X_SIG, X_GLYPH, W_GLYPH = 0.255, 0.445, 0.70, 0.285
    VMAX = 0.8  # shared value axis, so the continuous glyphs compare

    def value_axis(ax):
        ax.set_xlim(0, VMAX)
        ax.set_yticks([])
        for side in ("top", "left", "right"):
            ax.spines[side].set_visible(False)
        ax.spines["bottom"].set_color("#cccccc")
        ax.tick_params(axis="x", labelsize=8, colors=MUT, length=2.5, pad=1.5)

    def header(y, title, note):
        fig.text(X_FLAG, y, title, size=11, weight="bold", va="center")
        fig.text(X_FLAG + 0.205, y, note, size=9, color=MUT, va="center")
        fig.add_artist(
            plt.Line2D([X_FLAG, 0.985], [y - 0.018] * 2, color="#dddddd", lw=0.8)
        )

    def row(y, flag, sig, val, draw, h=0.042):
        fig.text(X_FLAG, y, flag, size=10, family=MONO, va="center")
        fig.text(X_SIG, y + 0.012, sig, size=8.5, family=MONO, va="center")
        fig.text(X_SIG, y - 0.012, val, size=8.5, family=MONO, color=MUT, va="center")
        draw(fig.add_axes([X_GLYPH, y - h / 2, W_GLYPH, h]))

    def binned(run, n_label):
        v = band(run)
        left, right = np.asarray(v["left"]), np.asarray(v["right"])
        counts = np.asarray(v["counts"], dtype=float)
        keep = counts > 0

        def draw(ax):
            # Neutral: bar height is a count, not a raster value.
            # 2 px of surface between adjacent bars.
            ax.bar(
                (left[keep] + right[keep]) / 2,
                counts[keep],
                width=(right[keep] - left[keep]) * 0.86,
                color=GLYPH,
                linewidth=0,
            )
            ax.set_ylim(0, counts.max() * 1.35)
            ax.text(
                0.995,
                0.9,
                n_label,
                transform=ax.transAxes,
                ha="right",
                va="top",
                size=8,
                color=MUT,
            )
            value_axis(ax)

        return draw, list(counts[keep].astype(int))

    def sample_pair(key, colour_of):
        """Two mini-maps of the oversampled cells lying in this one.

        Clipped to the outline: selection is by centre, so the raw boundary is
        ragged in a way that reads as a rendering fault.
        """
        outline = Path(np.asarray(demo.exterior.coords))

        def draw(ax):
            ax.set_axis_off()
            for i, m in enumerate(("nn", "bilinear")):
                sub = samp[f"{key}{m}"]
                a = ax.inset_axes([i * 0.52, 0.0, 0.46, 1.0])
                coll = PolyCollection(
                    [np.asarray(p.exterior.coords) for p in sub.geometry],
                    facecolors=colour_of(sub["band_1"].to_numpy()),
                    edgecolors="none",
                    rasterized=True,
                )
                a.add_collection(coll)
                a.set_xlim(cx - 250, cx + 250), a.set_ylim(cy - 250, cy + 250)
                clip = PathPatch(outline, transform=a.transData, fc="none", lw=0)
                a.add_patch(clip)
                coll.set_clip_path(clip)
                a.set_aspect("equal"), a.set_xticks([]), a.set_yticks([])
                a.set_frame_on(False)
                a.text(
                    0.5,
                    -0.06,
                    f"--sample {m}",
                    transform=a.transAxes,
                    ha="center",
                    va="top",
                    size=8,
                    family=MONO,
                    color=MUT,
                )

        return draw

    # ===== block 1: the continuous surface ================================
    header(
        0.912,
        "From the continuous surface",
        f"\u00b7 --point sees {n_pt} pixel centres; --overlay sees {n_ov} pixels",
    )

    def g_agg(ax):
        value_axis(ax)
        ax.plot(
            [agg["min"], agg["max"]],
            [0.5, 0.5],
            color=GLYPH,
            lw=5,
            solid_capstyle="round",
        )
        ax.plot(
            [agg["mean"]], [0.5], "o", ms=8, color="white", mec=MUT, mew=1.8, zorder=3
        )
        ax.set_ylim(0, 1)
        ax.text(agg["min"], 0.88, "min", size=7.5, color=MUT, ha="center")
        ax.text(agg["max"], 0.88, "max", size=7.5, color=MUT, ha="center")
        ax.text(agg["mean"], 0.0, "mean", size=7.5, color=MUT, ha="center")

    row(
        0.868,
        "--point -a min,max,mean",
        "struct<min, max, mean>",
        f"{{{agg['min']}, {agg['max']}, {agg['mean']}}}",
        g_agg,
    )

    def g_list(ax):
        value_axis(ax)
        ax.vlines(lst, 0.18, 0.82, color=GLYPH, lw=1.1)
        ax.set_ylim(0, 1)

    row(
        0.808,
        "--point list",
        "list<double>",
        f"[{lst[0]}, {lst[1]}, {lst[2]}, \u2026 {lst[-1]}]   n={len(lst)}",
        g_list,
    )

    draw_pt, counts_pt = binned("struct_hist", f"{n_pt} pixel centres")
    row(
        0.748,
        "--point histogram\n  --hist-width 0.1",
        "struct<left, right, counts>",
        f"counts: {counts_pt}",
        draw_pt,
    )

    draw_ov, counts_ov = binned("struct_ovhist", f"{n_ov} pixels, area-weighted")
    row(
        0.688,
        "--overlay histogram\n  --hist-width 0.1",
        "struct<left, right, counts>",
        f"counts: {counts_ov}",
        draw_ov,
    )

    row(
        0.605,
        f"--sample bilinear\n  -r {SAMPLE_RES}",
        "double",
        f"One scalar per cell \u00b7 --agg ignored\n{n_samp} cells centred inside this one",
        sample_pair("", lambda v: cover(np.clip(v, 0, 1))),
        h=0.088,
    )
    fig.text(
        X_SIG,
        0.529,
        f"Oversampling: r{SAMPLE_RES} cells are finer than the {NLM_PX:.0f} m pixels, so the "
        "kernel does the work \u2014 nn steps, bilinear interpolates",
        size=8,
        color=FAINT,
        va="center",
    )

    # ===== block 2: the classified surface ================================
    header(
        0.508,
        "From the classified surface",
        "\u00b7 Only three of the four classes fall in this cell",
    )

    def g_cathist(ax):
        for v, c in zip(cat_vals, cat_counts, strict=True):
            # class identity is a real encoding, so these bars keep the ramp
            ax.bar(v, c, width=0.55, color=cover(CLASS_STEPS[v - 1]), linewidth=0)
            ax.text(
                v, c + max(cat_counts) * 0.08, str(c), ha="center", size=7.5, color=MUT
            )
        ax.set_xlim(0.4, 4.6), ax.set_ylim(0, max(cat_counts) * 1.45)
        ax.set_xticks(cat_vals)
        ax.set_xticklabels([CLASS_NAME[v] for v in cat_vals])
        ax.set_yticks([])
        for side in ("top", "left", "right"):
            ax.spines[side].set_visible(False)
        ax.spines["bottom"].set_color("#cccccc")
        ax.tick_params(axis="x", labelsize=8, colors=MUT, length=0, pad=2)

    row(
        0.462,
        "--overlay histogram",
        "struct<values, counts>",
        f"{{values: {cat_vals}, counts: {cat_counts}}}",
        g_cathist,
        h=0.050,
    )
    fig.text(
        X_SIG,
        0.422,
        "No --hist-width, so it counts exact values \u2014 one bin per class present",
        size=8,
        color=FAINT,
        va="center",
    )

    def g_frac(ax):
        x = 0.0
        for c, f in zip(classes, fracs, strict=True):
            ax.barh(
                0,
                max(f - 0.006, 0.002),  # 2 px of surface between segments
                left=x,
                height=0.5,
                color=cover(CLASS_STEPS[c - 1]),
                linewidth=0,
            )
            ax.text(
                x + f / 2, -0.52, f"{f:.0%}", ha="center", va="top", size=7.5, color=MUT
            )
            x += f
        ax.set_xlim(0, 1), ax.set_ylim(-1.15, 0.45)
        ax.set_xticks([]), ax.set_yticks([]), ax.set_frame_on(False)

    row(
        0.388,
        "--overlay fractions",
        "struct<classes, fractions>",
        "{" + ", ".join(f"{c}: {f}" for c, f in zip(classes, fracs, strict=True)) + "}",
        g_frac,
    )
    fig.text(
        X_SIG,
        0.348,
        "Area each class covers \u2014 area-weighted, so not the counts above rescaled",
        size=8,
        color=FAINT,
        va="center",
    )

    def g_mode(ax):
        ax.barh(0, 0.16, height=0.5, color=cover(CLASS_STEPS[mode - 1]), linewidth=0)
        ax.text(
            0.19,
            0,
            f"Class {mode} \u2014 {CLASS_NAME[mode]} (largest overlap area)",
            va="center",
            size=8.5,
            color=MUT,
        )
        ax.set_xlim(0, 1), ax.set_ylim(-0.5, 0.5)
        ax.set_xticks([]), ax.set_yticks([]), ax.set_frame_on(False)

    row(0.315, "--overlay mode", "int64", f"{mode}", g_mode)

    cat_norm = matplotlib.colors.Normalize(vmin=0.5, vmax=4.5)
    row(
        0.232,
        f"--sample nn\n  -r {SAMPLE_RES}",
        "double",
        "One scalar per cell \u00b7 --agg ignored\nClass codes are labels, not "
        "measurements",
        sample_pair(
            "cat",
            lambda v: cover(
                np.interp(cat_norm(v), [0, 1], [CLASS_STEPS[0], CLASS_STEPS[-1]])
            ),
        ),
        h=0.088,
    )
    fig.text(
        X_SIG,
        0.158,
        f"nn keeps {len(set(samp['catnn']['band_1']))} real classes; bilinear averages "
        f"'grass' and 'shrub' into 2.4, inventing {n_cat_distinct:,} values that are no "
        "class at all",
        size=8,
        color=NOTE,
        va="center",
    )

    # ===== the footnote: what -d actually reaches =========================
    fig.add_artist(plt.Line2D([X_FLAG, 0.985], [0.145] * 2, color="#dddddd", lw=0.8))
    fig.text(X_FLAG, 0.126, "-d / --decimals", size=10, family=MONO, va="center")
    fig.text(
        X_SIG,
        0.126,
        "Rounds every number a schema emits \u2014 struct members, list elements and "
        "fractions alike,",
        size=8.5,
        color=MUT,
        va="center",
    )
    fig.text(
        X_SIG,
        0.104,
        "not only scalar band values. On a classified raster the obvious -d 0 is a trap:",
        size=8.5,
        color=MUT,
        va="center",
    )
    for i, (flag, vals, note) in enumerate(
        [
            ("-d 3", fracs, "As shown above"),
            ("-d 0", fracs_d0, "Every fraction collapses to 0 or 1"),
        ]
    ):
        y = 0.078 - i * 0.022
        fig.text(X_SIG, y, flag, size=8.5, family=MONO, color=INK, va="center")
        fig.text(
            X_SIG + 0.033,
            y,
            "{"
            + ", ".join(f"{c}: {f}" for c, f in zip(classes, vals, strict=True))
            + "}",
            size=8.5,
            family=MONO,
            color=MUT,
            va="center",
        )
        fig.text(X_SIG + 0.20, y, note, size=8, color=FAINT, va="center")

    fig.text(
        0.985,
        0.018,
        "raster2dggs \u00b7 synthetic neutral landscape model "
        f"(make_samples.nlm_fractal, seed {NLM_SEED})",
        size=8.5,
        color=FAINT,
        ha="right",
    )
    fig.savefig(f"{HERE}/struct-outputs.png", dpi=100)
    plt.close(fig)


def quantise(paths):
    """Palette-quantise the PNGs in place when it saves space and PIL is present."""
    try:
        from PIL import Image
    except ImportError:
        return
    for p in paths:
        q = p + ".q"
        Image.open(p).convert("RGB").quantize(
            colors=255, dither=Image.Dither.FLOYDSTEINBERG
        ).save(q, format="PNG", optimize=True)
        if os.path.getsize(q) < os.path.getsize(p):
            shutil.move(q, p)
        else:
            os.remove(q)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--workdir", help="keep/reuse intermediate data here (default: temp dir)"
    )
    args = ap.parse_args()
    w = args.workdir or tempfile.mkdtemp(prefix="raster2dggs-figs-")
    os.makedirs(f"{w}/runs", exist_ok=True)
    print(f"workdir: {w}")
    acquire(w)
    synth(w)
    index_all(w)
    fig_hero(w, Style(f"{w}/hero.tif"))
    fig_sampling(w, Style(f"{w}/as24_edge.tif"))
    fig_compaction(w)
    fig_structs(w)
    figs = (
        "raster2dggs-example",
        "sampling-strategies",
        "compaction-example",
        "struct-outputs",
    )
    quantise([f"{HERE}/{n}.png" for n in figs])
    for n in figs:
        print(f"{n}.png  {os.path.getsize(f'{HERE}/{n}.png') / 1024:.0f} KB")


if __name__ == "__main__":
    main()
