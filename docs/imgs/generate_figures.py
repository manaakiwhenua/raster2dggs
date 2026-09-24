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
from matplotlib.patches import Patch

HERE = os.path.dirname(os.path.abspath(__file__))

NZ_IMAGERY = "/vsicurl/https://nz-imagery.s3.ap-southeast-2.amazonaws.com"
BM35 = f"{NZ_IMAGERY}/manawatu-whanganui/manawatu-whanganui_2021-2022_0.3m/rgbnir/2193/BM35_10000_0402.tiff"
AS24 = f"{NZ_IMAGERY}/new-zealand/new-zealand_2024-2025_10m/rgb/2193/AS24.tiff"
WORLDCOVER = (
    "/vsis3/esa-worldcover/v200/2021/map/ESA_WorldCover_10m_2021_v200_S45W177_Map.tif"
)

INK, MUT, FAINT, NOTE = "#1a1a1a", "#555555", "#888888", "#b0332a"
MASKC = "#ebebeb"
# ESA WorldCover class palette (their standard colours); identity is also
# carried by the legend, and the three classes separate in lightness.
WC_CLASSES = {
    10: ("Tree cover", "#006400"),
    30: ("Grassland", "#ffff4c"),
    80: ("Open water", "#0064c8"),
}
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


def index_all(w):
    hero, strip_src = f"{w}/hero.tif", f"{w}/hero_strip.tif"
    edge, wc = f"{w}/as24_edge.tif", f"{w}/worldcover.tif"
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
        "every pixel lands in the DGGS cell containing its centre, and each cell aggregates its pixels' band values\n"
        "a finer resolution keeps more of the image — and costs proportionally more rows",
        size=11,
        va="top",
        linespacing=1.5,
    )

    n13 = 0
    for x, (title, run) in zip(
        (0.018, 0.352, 0.686),
        [
            ("raster input", None),
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
        "one argument picks the grid — the rest of the command is unchanged",
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
        "pixels in  ·  one row per cell out",
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
        "on disk  ·  hive-partitioned by parent cell  ·  -c brotli",
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
        f"a reader can skip whole partitions without opening them\n"
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
        "three ways to move pixel values onto cells",
        size=15,
        weight="bold",
        va="top",
    )
    fig.text(
        0.018,
        0.885,
        "the choice matters most at edges — here, a mask boundary in the LINZ 10 m mosaic "
        "(zoomed; H3 resolution 12, cells ≈ 2 × 2 pixels)",
        size=11,
        color=MUT,
        va="top",
    )
    panels = [
        (None, "input (zoom)", "grey = masked (alpha 0)"),
        (
            "samp_point",
            "--point value",
            "cell centre's pixel value\nfast, exact at native scale",
        ),
        (
            "samp_overlay",
            "--overlay weighted",
            "area-weighted mean of every\nvalid pixel the cell touches",
        ),
        (
            "samp_bilinear",
            "--sample bilinear",
            "kernel interpolation at the\ncell centre (bicubic, lanczos…)",
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
            ("comp", "as indexed · resolution 12"),
            ("comp_co", "with --compact · mixed resolutions"),
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
    index_all(w)
    fig_hero(w, Style(f"{w}/hero.tif"))
    fig_sampling(w, Style(f"{w}/as24_edge.tif"))
    fig_compaction(w)
    quantise(
        [
            f"{HERE}/{n}.png"
            for n in (
                "raster2dggs-example",
                "sampling-strategies",
                "compaction-example",
            )
        ]
    )
    for n in ("raster2dggs-example", "sampling-strategies", "compaction-example"):
        print(f"{n}.png  {os.path.getsize(f'{HERE}/{n}.png') / 1024:.0f} KB")


if __name__ == "__main__":
    main()
