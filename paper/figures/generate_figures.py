"""Regenerate the JOSS paper figures.

Indexes two public datasets with the raster2dggs and vector2dggs CLIs and
renders four manuscript figures into this directory, one concept each:

- indexing-example.png   input and two H3 resolutions, raster above vector
- grids-example.png      the same two inputs on five grid systems
- output-table.md        the head of each output table, pasted into paper.md
- output-size.png        output size by encoding, for both tools
- raster-transfer-strategies.png  the three raster transfer rules at a mask edge

Needs raster2dggs[all] and vector2dggs[all] on PATH (or in the venvs named by
RASTER2DGGS / VECTOR2DGGS), matplotlib, pillow, gdal_translate and network
access for the first run. Intermediate data is kept in --workdir.

    python paper/figures/generate_figures.py --workdir /tmp/paper-figs

Sources (all CC BY 4.0): LINZ 0.3 m aerial imagery, Manawatū-Whanganui
2021-2022, tile BM35; LINZ 10 m national satellite mosaic 2024-2025, tile AS24,
whose extent crosses the mosaic's coverage edge; LCDB v5.0 land cover for
Rangatira (South East Island), the vector2dggs test fixture; ESA WorldCover
10 m 2021 v200 over the same island, as the categorical raster.
"""

import argparse
import io
import os
import shutil
import subprocess
import tempfile

import matplotlib

matplotlib.use("Agg")
import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import rasterio
import shapely
from matplotlib.collections import PolyCollection
from matplotlib.legend_handler import HandlerTuple
from matplotlib.patches import Patch

HERE = os.path.dirname(os.path.abspath(__file__))
RASTER2DGGS = os.environ.get("RASTER2DGGS", "raster2dggs")
VECTOR2DGGS = os.environ.get("VECTOR2DGGS", "vector2dggs")

BM35 = (
    "/vsicurl/https://nz-imagery.s3.ap-southeast-2.amazonaws.com/manawatu-whanganui"
    "/manawatu-whanganui_2021-2022_0.3m/rgbnir/2193/BM35_10000_0402.tiff"
)
AS24 = (
    "/vsicurl/https://nz-imagery.s3.ap-southeast-2.amazonaws.com/new-zealand"
    "/new-zealand_2024-2025_10m/rgb/2193/AS24.tiff"
)
WORLDCOVER = (
    "/vsis3/esa-worldcover/v200/2021/map/ESA_WorldCover_10m_2021_v200_S45W177_Map.tif"
)
LCDB = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(HERE))),
    "vector2dggs",
    "tests",
    "data",
    "se-island.gpkg",
)
LCDB = os.environ.get("LCDB_GPKG", LCDB)
LCDB_LAYER, ISLAND_CRS = "se-island", "EPSG:3793"

RASTER_RES = (12, 13)
VECTOR_RES = (10, 12)
# The tables compare both tools at one H3 resolution. The size chart uses S2 at
# a level of similar cell area (~310 m²): with aperture 4, a complete set of
# identical siblings is far more likely than with H3's aperture 7, so
# compaction has more to merge.
TABLE_RES = 12
SIZE_DGGS, SIZE_RES, SIZE_PARENT_RES = "s2", 19, 10
# Grid and resolution pairs for the five-grid figure, chosen so cells are a
# legible size over each window.
RASTER_STRIP = [("h3", 12), ("rhp", 13), ("a5", 19), ("s2", 18), ("geohash", 8)]
VECTOR_STRIP = [("h3", 10), ("rhp", 11), ("a5", 17), ("s2", 16), ("geohash", 7)]
# flag sets for the transfer-strategy figure, on the mosaic's coverage edge
TRANSFERS = [
    ("point", ("--point", "value")),
    ("overlay", ("--overlay", "weighted")),
    ("bilinear", ("--sample", "bilinear")),
]
MASKC = "#ebebeb"
GRID_NAMES = {
    "h3": "H3",
    "rhp": "rHEALPix",
    "a5": "A5",
    "s2": "S2",
    "geohash": "Geohash",
}

LC_COLOURS = {
    "Indigenous Forest": "#1b6e3a",
    "Broadleaved Indigenous Hardwoods": "#5fc48a",
    "Low Producing Grassland": "#e8b43a",
    "Gravel or Rock": "#a79bd6",
}
INK, MUT, FAINT = "#1a1a1a", "#555555", "#999999"
MONO = "DejaVu Sans Mono"
RASTER_C, VECTOR_C = "#2f6fb7", "#c0392b"
TEXT_WIDTH_IN = 6.8  # figures are placed at the full text width

plt.rcParams.update(
    {
        "font.family": "DejaVu Sans",
        "font.size": 8,
        "text.color": INK,
        "axes.edgecolor": "#bbbbbb",
        "axes.linewidth": 0.6,
        "figure.facecolor": "white",
        "savefig.dpi": 300,
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


RGB = ("-b", "1", "-b", "2", "-b", "3", "-d", "0")
# every size-chart run writes native integer cell IDs, as one partition so that
# per-file overhead does not swamp the encodings compared; the tables keep strings
U64 = ("--cell-id", "uint64", "-pr", str(SIZE_PARENT_RES))


def r2d(dggs, src, out, res, *extra, bands=RGB):
    sh(RASTER2DGGS, dggs, src, out, "-r", str(res), "-p", "8", "-o", *bands, *extra)


def v2d(w, dggs, out, res, *extra):
    sh(
        VECTOR2DGGS,
        dggs,
        f"{w}/island.gpkg",
        out,
        "-r",
        str(res),
        "-p",
        "4",
        "-o",
        "-v",
        "WARNING",
        "-id",
        "LCDB_UID",
        "-ka",
        "Name_2018",
        *extra,
    )


def co_flag(co):
    return ("-co",) if co else ()


def index_all(w):
    hero, strip, edge = f"{w}/hero.tif", f"{w}/hero_strip.tif", f"{w}/as24_edge.tif"
    fetch(BM35, hero, "-srcwin", "4000", "3000", "4096", "4096")
    fetch(BM35, strip, "-srcwin", "5200", "4600", "1024", "1024")
    fetch(AS24, edge, "-srcwin", "1152", "0", "512", "512")
    wc = f"{w}/worldcover.tif"
    fetch(WORLDCOVER, wc, "-projwin", "-176.188", "-44.333", "-176.158", "-44.359")
    if not os.path.exists(f"{w}/island.gpkg"):
        island()[["LCDB_UID", "Name_2018", "geometry"]].to_file(f"{w}/island.gpkg")
    r_lo, r_hi = RASTER_RES
    v_lo, v_hi = VECTOR_RES
    tr = TABLE_RES
    jobs = {
        f"runs/hero_h3_{r_lo}": lambda o: r2d("h3", hero, o, r_lo, "-g", "polygon"),
        f"runs/hero_h3_{r_hi}": lambda o: r2d("h3", hero, o, r_hi, "-g", "polygon"),
        "runs/disk_default": lambda o: r2d(SIZE_DGGS, hero, o, SIZE_RES, *U64),
        "runs/disk_none": lambda o: r2d(
            SIZE_DGGS, hero, o, SIZE_RES, "-c", "none", *U64
        ),
        "runs/disk_zstd": lambda o: r2d(
            SIZE_DGGS, hero, o, SIZE_RES, "-c", "zstd", *U64
        ),
        "runs/disk_point": lambda o: r2d(
            SIZE_DGGS, hero, o, SIZE_RES, "-g", "point", *U64
        ),
        **{
            f"runs/strip_{d}": (
                lambda o, d=d, r=r: r2d(d, strip, o, r, "-g", "polygon")
            )
            for d, r in RASTER_STRIP
        },
        f"vruns/h3_{v_lo}": lambda o: v2d(w, "h3", o, v_lo, "--geo", "polygon"),
        f"vruns/h3_{v_hi}": lambda o: v2d(w, "h3", o, v_hi, "--geo", "polygon"),
        "vruns/disk_default": lambda o: v2d(w, SIZE_DGGS, o, SIZE_RES, *U64),
        "vruns/disk_none": lambda o: v2d(
            w, SIZE_DGGS, o, SIZE_RES, "-cp", "none", *U64
        ),
        "vruns/disk_zstd": lambda o: v2d(
            w, SIZE_DGGS, o, SIZE_RES, "-cp", "zstd", *U64
        ),
        "vruns/disk_point": lambda o: v2d(
            w, SIZE_DGGS, o, SIZE_RES, "--geo", "point", *U64
        ),
        **{
            f"vruns/strip_{d}": (
                lambda o, d=d, r=r: v2d(w, d, o, r, "--geo", "polygon")
            )
            for d, r in VECTOR_STRIP
        },
        **{
            f"vruns/strip_co_{d}": (
                lambda o, d=d, r=r: v2d(w, d, o, r, "--geo", "polygon", "-co")
            )
            for d, r in VECTOR_STRIP
        },
        "runs/agg": lambda o: r2d("h3", hero, o, tr, "-a", "min,max,mean"),
        "runs/table": lambda o: r2d("h3", hero, o, tr),
        "vruns/table": lambda o: v2d(w, "h3", o, tr),
        # categorical raster: majority class per cell, so siblings can agree
        **{
            f"cruns/{'disk' if co and name == 'disk_default' else name}{co}": (
                lambda o, a=a, co=co: r2d(
                    SIZE_DGGS,
                    wc,
                    o,
                    SIZE_RES,
                    "-d",
                    "0",
                    "--overlay",
                    "mode",
                    *a,
                    *co_flag(co),
                    *U64,
                    bands=(),
                )
            )
            for name, a in (
                ("disk_default", ()),
                ("disk_none", ("-c", "none")),
                ("disk_zstd", ("-c", "zstd")),
                ("disk_point", ("-g", "point")),
                ("polygon", ("-g", "polygon")),
            )
            for co in ("", "_co")
        },
        "cruns/frac": lambda o: r2d(
            "h3", wc, o, tr, "-d", "3", "--overlay", "fractions", bands=()
        ),
        "runs/disk_co": lambda o: r2d(SIZE_DGGS, hero, o, SIZE_RES, "-co", *U64),
        "runs/disk_none_co": lambda o: r2d(
            SIZE_DGGS, hero, o, SIZE_RES, "-c", "none", "-co", *U64
        ),
        "runs/disk_zstd_co": lambda o: r2d(
            SIZE_DGGS, hero, o, SIZE_RES, "-c", "zstd", "-co", *U64
        ),
        "runs/disk_point_co": lambda o: r2d(
            SIZE_DGGS, hero, o, SIZE_RES, "-g", "point", "-co", *U64
        ),
        "runs/polygon": lambda o: r2d(
            SIZE_DGGS, hero, o, SIZE_RES, "-g", "polygon", *U64
        ),
        "runs/polygon_co": lambda o: r2d(
            SIZE_DGGS, hero, o, SIZE_RES, "-g", "polygon", "-co", *U64
        ),
        "vruns/disk_co": lambda o: v2d(w, SIZE_DGGS, o, SIZE_RES, "-co", *U64),
        "vruns/disk_none_co": lambda o: v2d(
            w, SIZE_DGGS, o, SIZE_RES, "-cp", "none", "-co", *U64
        ),
        "vruns/disk_zstd_co": lambda o: v2d(
            w, SIZE_DGGS, o, SIZE_RES, "-cp", "zstd", "-co", *U64
        ),
        "vruns/disk_point_co": lambda o: v2d(
            w, SIZE_DGGS, o, SIZE_RES, "--geo", "point", "-co", *U64
        ),
        "vruns/polygon": lambda o: v2d(
            w, SIZE_DGGS, o, SIZE_RES, "--geo", "polygon", *U64
        ),
        "vruns/polygon_co": lambda o: v2d(
            w, SIZE_DGGS, o, SIZE_RES, "--geo", "polygon", "-co", *U64
        ),
        **{
            f"runs/samp_{k}": (
                lambda o, a=a: r2d("h3", edge, o, 12, *a, "-g", "polygon")
            )
            for k, a in TRANSFERS
        },
    }
    for name, job in jobs.items():
        out = f"{w}/{name}"
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
            self.shape = (s.width, s.height)
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

    def extent(self):
        b = self.bounds
        return (b.left, b.right, b.bottom, b.top)


def cells(path, crs):
    df = pq.read_table(path).to_pandas()
    g = gpd.GeoDataFrame(
        df.drop(columns=["geometry"]),
        geometry=gpd.GeoSeries(
            shapely.from_wkb(df["geometry"]), index=df.index, crs=4326
        ),
    )
    return g.to_crs(crs)


def plot_cells(ax, g, colors, lw=0.0, ec="none"):
    verts = [np.asarray(p.exterior.coords) for p in g.geometry]
    ax.add_collection(
        PolyCollection(
            verts, facecolors=colors, edgecolors=ec, linewidths=lw, rasterized=True
        )
    )


def band_colors(style, g):
    return style.stretch(np.c_[g["band_1"], g["band_2"], g["band_3"]])


def lc_colors(g):
    return [LC_COLOURS[n] for n in g["Name_2018"]]


def island():
    return gpd.read_file(LCDB, layer=LCDB_LAYER).to_crs(ISLAND_CRS)


def square_bounds(gdf, pad=0.04):
    """A square window around the data, so island panels match the raster's aspect."""
    x0, y0, x1, y1 = gdf.total_bounds
    cx, cy, half = (x0 + x1) / 2, (y0 + y1) / 2, max(x1 - x0, y1 - y0) / 2 * (1 + pad)
    return (cx - half, cx + half, cy - half, cy + half)


def frame(ax, xlim, ylim, title=None):
    ax.set_xlim(*xlim), ax.set_ylim(*ylim)
    ax.set_aspect("equal"), ax.set_xticks([]), ax.set_yticks([])
    if title:
        ax.set_title(title, size=8, pad=3)


def row_label(ax, text, colour):
    ax.text(
        -0.06,
        0.5,
        text,
        transform=ax.transAxes,
        rotation=90,
        ha="center",
        va="center",
        size=8.5,
        weight="bold",
        color=colour,
    )


def scalebar(ax, metres, label, xlim, ylim):
    x0 = xlim[0] + (xlim[1] - xlim[0]) * 0.05
    y0 = ylim[0] + (ylim[1] - ylim[0]) * 0.05
    ax.plot([x0, x0 + metres], [y0, y0], color=INK, lw=1.2, solid_capstyle="butt")
    ax.text(
        x0 + metres / 2, y0 + (ylim[1] - ylim[0]) * 0.02, label, ha="center", size=6.5
    )


def dir_kb(d):
    return round(
        sum(os.path.getsize(os.path.join(r, f)) for r, _, fs in os.walk(d) for f in fs)
        / 1024
    )


def dir_kb_encoded(d, cell_col):
    """Size of a Parquet store written sorted by cell ID with a delta-encoded ID column.

    This is what manaakiwhenua/raster2dggs#134 and manaakiwhenua/vector2dggs#240
    propose as the default write path (and it supersedes raster2dggs#133). Until
    they land, the files are re-encoded here with pyarrow, keeping each file's
    own compression codec, so the chart matches the paper's description.
    """
    total = 0
    for part in sorted(p for p in os.listdir(d) if "=" in p):
        files = sorted(f for f in os.listdir(f"{d}/{part}") if f.endswith(".parquet"))
        t = pa.concat_tables([pq.read_table(f"{d}/{part}/{f}") for f in files])
        codec = (
            pq.ParquetFile(f"{d}/{part}/{files[0]}")
            .metadata.row_group(0)
            .column(0)
            .compression.lower()
        )
        integer = pa.types.is_integer(t.schema.field(cell_col).type)
        buf = io.BytesIO()
        pq.write_table(
            t.sort_by(cell_col),
            buf,
            compression="none" if codec == "uncompressed" else codec,
            use_dictionary=[c for c in t.column_names if c != cell_col],
            column_encoding={
                cell_col: "DELTA_BINARY_PACKED" if integer else "DELTA_BYTE_ARRAY"
            },
        )
        total += buf.tell()
    return round(total / 1024)


def legend(fig, y):
    fig.legend(
        handles=[Patch(fc=c, label=n) for n, c in LC_COLOURS.items()],
        loc="lower center",
        bbox_to_anchor=(0.5, y),
        ncol=4,
        frameon=False,
        fontsize=7,
        handlelength=1.2,
        columnspacing=1.4,
    )


def fig_indexing(w):
    style = Style(f"{w}/hero.tif")
    isl = island()
    r_lo, r_hi = RASTER_RES
    v_lo, v_hi = VECTOR_RES
    fig, axes = plt.subplots(
        2, 3, figsize=(TEXT_WIDTH_IN, 4.75), gridspec_kw=dict(wspace=0.06, hspace=0.18)
    )
    fig.subplots_adjust(left=0.035, right=0.995, top=0.95, bottom=0.075)

    ext = style.extent()
    xl, yl = (ext[0], ext[1]), (ext[2], ext[3])
    ax = axes[0, 0]
    ax.imshow(style.display_image(), extent=ext)
    frame(ax, xl, yl, "Input: 0.3 m aerial imagery")
    scalebar(ax, 300, "300 m", xl, yl)
    for ax, r in zip(axes[0, 1:], RASTER_RES, strict=True):
        g = cells(f"{w}/runs/hero_h3_{r}", "EPSG:2193")
        plot_cells(ax, g, band_colors(style, g))
        frame(ax, xl, yl, f"H3 resolution {r}: {len(g):,} cells")
    row_label(axes[0, 0], "raster2dggs", RASTER_C)

    xl, yl = square_bounds(isl)[:2], square_bounds(isl)[2:]
    ax = axes[1, 0]
    isl.plot(ax=ax, color=lc_colors(isl), edgecolor="white", linewidth=0.3)
    frame(ax, xl, yl, f"Input: {len(isl)} land-cover polygons")
    scalebar(ax, 500, "500 m", xl, yl)
    for ax, r in zip(axes[1, 1:], VECTOR_RES, strict=True):
        g = cells(f"{w}/vruns/h3_{r}", ISLAND_CRS)
        plot_cells(ax, g, lc_colors(g), lw=0.25 if r == v_lo else 0.0, ec="white")
        frame(ax, xl, yl, f"H3 resolution {r}: {len(g):,} cells")
    row_label(axes[1, 0], "vector2dggs", VECTOR_C)
    legend(fig, 0.0)
    fig.savefig(f"{HERE}/indexing-example.png")
    plt.close(fig)


def fig_grids(w):
    style = Style(f"{w}/hero_strip.tif")
    isl = island()
    fig, axes = plt.subplots(
        3, 5, figsize=(TEXT_WIDTH_IN, 4.7), gridspec_kw=dict(wspace=0.06, hspace=0.32)
    )
    fig.subplots_adjust(left=0.035, right=0.995, top=0.93, bottom=0.06)
    ext = style.extent()
    xl, yl = (ext[0], ext[1]), (ext[2], ext[3])
    for ax, (d, r) in zip(axes[0], RASTER_STRIP, strict=True):
        g = cells(f"{w}/runs/strip_{d}", "EPSG:2193")
        plot_cells(ax, g, band_colors(style, g))
        frame(ax, xl, yl, f"{GRID_NAMES[d]} res. {r}")
    row_label(axes[0, 0], "raster2dggs", RASTER_C)
    sb = square_bounds(isl)
    xl, yl = sb[:2], sb[2:]
    for ax, (d, r) in zip(axes[1], VECTOR_STRIP, strict=True):
        g = cells(f"{w}/vruns/strip_{d}", ISLAND_CRS)
        plot_cells(ax, g, lc_colors(g), lw=0.2, ec="white")
        frame(ax, xl, yl, f"{GRID_NAMES[d]} res. {r}\n{len(g):,} cells")
    row_label(axes[1, 0], "vector2dggs", VECTOR_C)
    for ax, (d, _r) in zip(axes[2], VECTOR_STRIP, strict=True):
        g = cells(f"{w}/vruns/strip_co_{d}", ISLAND_CRS)
        plot_cells(ax, g, lc_colors(g), lw=0.2, ec="white")
        frame(ax, xl, yl, f"compacted\n{len(g):,} cells")
    row_label(axes[2, 0], "compacted", VECTOR_C)
    legend(fig, 0.0)
    fig.savefig(f"{HERE}/grids-example.png")
    plt.close(fig)


def table_rows(path, cols, n=5, keep=None):
    t = pq.read_table(path).to_pandas().reset_index().sort_values(cols[0])
    if keep is not None:
        t = t[t.apply(keep, axis=1)]

    def fmt(v):
        if isinstance(v, dict):
            return "{" + ", ".join(f"{k}: {fmt(x)}" for k, x in v.items()) + "}"
        if isinstance(v, (list, np.ndarray)):
            return "[" + ", ".join(fmt(x) for x in v) + "]"
        return f"{v:g}" if isinstance(v, float) else str(v)

    def cell(c, v):
        if isinstance(v, dict) and all(
            not isinstance(x, (list, np.ndarray)) for x in v.values()
        ):
            return "{" + ", ".join(fmt(x) for x in v.values()) + "}"
        return f"`{v}`" if c.startswith("h3_") else fmt(v)

    return [[cell(c, r[c]) for c in cols] for _, r in t.head(n).iterrows()], len(t)


def md_table(rows, cols, widths, caption):
    """A pandoc pipe table; dash counts set the relative column widths in LaTeX."""
    head = "| " + " | ".join(f"`{c}`" for c in cols) + " |"
    rule = "|" + "|".join("-" * wd for wd in widths) + "|"
    body = ["| " + " | ".join(r) + " |" for r in rows]
    return "\n".join([head, rule, *body, "", f": {caption}"])


def write_tables(w):
    """The output tables, as markdown to paste into paper.md."""
    r, pr = TABLE_RES, TABLE_RES - 6
    cid, pid = f"h3_{r:02d}", f"h3_{pr:02d}"
    rcols, vcols = [cid, pid, "band_1", "band_2", "band_3"], [
        cid,
        pid,
        "LCDB_UID",
        "Name_2018",
    ]
    plain, n_r = table_rows(f"{w}/runs/table", rcols)
    agg, _ = table_rows(f"{w}/runs/agg", rcols)
    vec, n_v = table_rows(f"{w}/vruns/table", vcols)
    ccols = [cid, pid, "band_1"]
    frac, _ = table_rows(
        f"{w}/cruns/frac", ccols, keep=lambda r: len(r["band_1"]["classes"]) > 1
    )
    n_c = pq.read_table(f"{w}/cruns/frac").num_rows
    tables = [
        md_table(
            plain,
            rcols,
            [20, 20, 9, 9, 9],
            f"The first rows of `raster2dggs` output for the resolution {r} imagery in "
            f"\\autoref{{fig:indexing}} ({n_r:,} rows): one row per cell, holding the cell "
            "identifier, the partition key (the cell's ancestor six levels coarser) and "
            "the mean of each band over the cell's pixels.\\label{tbl:raster}",
        ),
        md_table(
            agg,
            rcols,
            [18, 18, 16, 16, 16],
            "The same run with `-a min,max,mean`: each band becomes a struct column "
            "holding every requested aggregate, in order.\\label{tbl:raster-agg}",
        ),
        md_table(
            frac,
            ccols,
            [18, 18, 44],
            f"ESA WorldCover 10 m land cover over the same island as "
            f"\\autoref{{fig:indexing}}, indexed by `raster2dggs` with `--overlay fractions` "
            f"({n_c:,} rows, of which the first five with more than one class are shown): each "
            "cell holds the classes it overlaps and the fraction of its area under each, "
            "using WorldCover's class codes.\\label{tbl:raster-frac}",
        ),
        md_table(
            vec,
            vcols,
            [20, 20, 18, 22],
            f"The first rows of `vector2dggs` output for the resolution {r} land-cover "
            f"polygons in \\autoref{{fig:indexing}} ({n_v:,} rows): the identifier and "
            "attributes of the feature containing each cell.\\label{tbl:vector}",
        ),
    ]
    with open(f"{HERE}/output-table.md", "w") as f:
        f.write("\n\n".join(tables) + "\n")


def fig_size(w):
    """Each encoding as indexed and compacted; the input file once, for scale."""
    variants = [
        ("CSV", "csv"),
        ("Parquet, uncompressed", "disk_none"),
        ("Parquet, snappy (default)", "disk_default"),
        ("Parquet, zstd", "disk_zstd"),
        ("GeoParquet, point", "disk_point"),
        ("GeoParquet, polygon", "polygon"),
    ]
    sizes = {}
    for tool, runs, src in [
        ("raster", "runs", f"{w}/hero.tif"),
        ("categorical", "cruns", f"{w}/worldcover.tif"),
        ("vector", "vruns", f"{w}/island.gpkg"),
    ]:
        kb = {"input": os.path.getsize(src) / 1024}
        cell_col = f"{SIZE_DGGS}_{SIZE_RES:02d}"
        for co in ("", "_co"):
            csv = f"{w}/{runs}/table{co}.csv"
            if not os.path.exists(csv):
                src_run = (
                    f"{w}/{runs}/disk_default" if not co else f"{w}/{runs}/disk_co"
                )
                pq.read_table(src_run).sort_by(cell_col).to_pandas().to_csv(csv)
            kb["csv" + co] = os.path.getsize(csv) / 1024
            pairs = {
                "disk_default": ("disk_default", "disk_co"),
                "disk_none": ("disk_none", "disk_none_co"),
                "disk_zstd": ("disk_zstd", "disk_zstd_co"),
                "disk_point": ("disk_point", "disk_point_co"),
                "polygon": ("polygon", "polygon_co"),
            }
            for key, (plain, compacted) in pairs.items():
                kb[key + co] = dir_kb_encoded(
                    f"{w}/{runs}/{compacted if co else plain}", cell_col
                )
        sizes[tool] = kb

    def label(kb):
        return f"{kb / 1024:,.0f} MB" if kb >= 1024 else f"{kb:,.0f} KB"

    fig, axes = plt.subplots(1, 3, figsize=(TEXT_WIDTH_IN, 2.9), sharex=True)
    fig.subplots_adjust(left=0.2, right=0.99, top=0.87, bottom=0.19, wspace=0.12)
    lo, hi = 1, 2e5
    h = 0.38
    rows = ["Input file"] + [lbl for lbl, _ in variants]
    ys = np.arange(len(rows))[::-1]
    for ax, (tool, colour, title) in zip(
        axes,
        [
            ("raster", RASTER_C, "raster2dggs\nAerial imagery"),
            ("categorical", RASTER_C, "raster2dggs\nLand cover (categorical)"),
            ("vector", VECTOR_C, "vector2dggs\nLand cover (polygons)"),
        ],
        strict=True,
    ):
        kb = sizes[tool]
        ax.barh(ys[0], kb["input"] - lo, left=lo, height=h * 1.6, color=FAINT)
        ax.annotate(
            label(kb["input"]),
            (kb["input"], ys[0]),
            xytext=(3, 0),
            textcoords="offset points",
            va="center",
            size=6.5,
        )
        for y, (_, key) in zip(ys[1:], variants, strict=True):
            for dy, co, alpha in ((h / 2, "", 1.0), (-h / 2, "_co", 0.45)):
                v = kb[key + co]
                ax.barh(
                    y + dy, v - lo, left=lo, height=h, color=colour, alpha=alpha, lw=0
                )
                ax.annotate(
                    label(v),
                    (v, y + dy),
                    xytext=(3, 0),
                    textcoords="offset points",
                    va="center",
                    size=6.5,
                )
        ax.set_xscale("log"), ax.set_xlim(lo, hi)
        ax.set_ylim(-0.6, len(rows) - 0.4)
        ax.set_yticks(ys)
        ax.set_yticklabels(rows if ax is axes[0] else [], size=7)
        ax.set_xticks([1, 10, 100, 1024, 10240, 102400])
        ax.set_xticklabels(["1 KB", "10", "100", "1 MB", "10", "100"], size=6.5)
        ax.xaxis.set_minor_locator(matplotlib.ticker.NullLocator())
        for side in ("top", "right"):
            ax.spines[side].set_visible(False)
        ax.tick_params(axis="y", length=0)
        ax.tick_params(axis="x", length=2, colors=MUT)
        ax.set_title(title, size=8, color=colour, pad=4, loc="left")
    fig.legend(
        handles=[
            (Patch(fc=RASTER_C), Patch(fc=VECTOR_C)),
            (Patch(fc=RASTER_C, alpha=0.45), Patch(fc=VECTOR_C, alpha=0.45)),
        ],
        labels=["default", "compacted"],
        handler_map={tuple: HandlerTuple(ndivide=None, pad=0)},
        loc="lower center",
        bbox_to_anchor=(0.59, 0.0),
        ncol=2,
        frameon=False,
        fontsize=7,
        handlelength=2.0,
    )
    fig.savefig(f"{HERE}/output-size.png")
    plt.close(fig)
    return sizes


def fig_transfer(w):
    style = Style(f"{w}/as24_edge.tif")
    b = style.bounds
    # zoom straddling the mask boundary, centred on the boundary's mean row
    first_valid = np.argmax(style.alpha == 255, axis=0)
    c0, c1 = 140, 220
    mid = int(first_valid[c0:c1].mean())
    xl = (b.left + c0 * 10, b.left + c1 * 10)
    yl = (b.top - (mid + 40) * 10, b.top - (mid - 40) * 10)
    panels = [
        (None, "Input (zoom)", "grey = masked pixels"),
        ("point", "--point value", "pixel value at the cell centre"),
        (
            "overlay",
            "--overlay weighted",
            "area-weighted mean of the\nvalid pixels the cell touches",
        ),
        ("bilinear", "--sample bilinear", "kernel interpolation\nat the cell centre"),
    ]
    fig, axes = plt.subplots(
        1, 4, figsize=(TEXT_WIDTH_IN, 2.15), gridspec_kw=dict(wspace=0.06)
    )
    fig.subplots_adjust(left=0.01, right=0.99, top=0.9, bottom=0.17)
    for ax, (run, title, note) in zip(axes, panels, strict=True):
        if run is None:
            ax.imshow(style.display_image(), extent=style.extent())
        else:
            g = cells(f"{w}/runs/samp_{run}", "EPSG:2193")
            plot_cells(ax, g, band_colors(style, g))
        frame(ax, xl, yl)
        ax.set_title(title, size=8, family=MONO if run else "DejaVu Sans", pad=3)
        ax.text(
            0.5,
            -0.05,
            note,
            transform=ax.transAxes,
            ha="center",
            va="top",
            size=7,
            color=MUT,
            linespacing=1.3,
        )
    fig.savefig(f"{HERE}/raster-transfer-strategies.png")
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
    ap.add_argument("--no-quantise", action="store_true", help="keep full-colour PNGs")
    args = ap.parse_args()
    w = args.workdir or tempfile.mkdtemp(prefix="paper-figs-")
    print(f"workdir: {w}")
    index_all(w)
    fig_indexing(w)
    fig_grids(w)
    write_tables(w)
    sizes = fig_size(w)
    print(sizes)
    fig_transfer(w)
    figs = [
        "indexing-example",
        "grids-example",
        "output-size",
        "raster-transfer-strategies",
    ]
    if not args.no_quantise:
        quantise([f"{HERE}/{n}.png" for n in figs])
    for n in figs:
        print(f"{n}.png  {os.path.getsize(f'{HERE}/{n}.png') / 1024:.0f} KB")


if __name__ == "__main__":
    main()
