---
title: 'raster2dggs and vector2dggs: command-line tools for indexing geospatial data to discrete global grid systems'
tags:
  - Python
  - geospatial
  - discrete global grid systems
  - DGGS
  - Parquet
  - raster
  - vector
authors:
  - name: Richard Law
    orcid: 0000-0002-7400-2530
    corresponding: true
    affiliation: 1
  - name: James Ardo
    orcid: 0009-0008-1201-9733
    affiliation: 1
  - name: Nicoletta De Maio
    # orcid: null
    affiliation: 1
affiliations:
  - name: Bioeconomy Science Institute, New Zealand
    index: 1
    ror: 03j13xx78
date: 12 November 2026
bibliography: paper.bib
---

# Summary

A discrete global grid system (DGGS) divides the Earth's surface into a hierarchy of cells, each with a unique identifier [@sahr2003; @ogc2021]. Once two datasets are expressed on the same grid, combining them no longer needs geometry: it is a table join on the cell identifier. Getting data onto a grid is hard because real datasets are large, come as rasters (images, elevation models) or vectors (points, lines, polygons), and each grid system has its own library.

`raster2dggs` [@raster2dggs] and `vector2dggs` [@vector2dggs] are command-line tools for this conversion. Each reads any raster or vector source GDAL [@gdal] can open and writes a Parquet [@parquet] dataset with one row per cell at a chosen resolution. Both support H3 [@h3], S2 [@s2], rHEALPix [@gibb2016], A5 [@a5] and Geohash through one interface; `raster2dggs` adds Maidenhead and thirteen further grids through DGGAL [@dggal]. Both process inputs larger than memory, in parallel.

# Statement of need

DGGS are promoted as a common reference frame for integrating heterogeneous spatial data [@purss2019], and the grid libraries provide the primitives: the cell containing a point, the cells covering a polygon, parent-child relationships. A researcher must still read the source in pieces that fit in memory, reproject it, decide how a pixel or polygon maps to cells, reconcile cells that straddle the pieces, handle the antimeridian, and write an economical output that other tools can read, contending with a different API for each grid system.

The tools were written at the Bioeconomy Science Institute[^1] to integrate national-scale environmental layers (land cover, elevation, cadastral parcels) on a common grid; to compare grid systems, whose distortion properties differ [@kmoch2022]; to produce cell-keyed feature tables for machine learning; and to publish DGGS-indexed institutional datasets. The audience is researchers and data engineers who want an efficient DGGS representation of their geospatial data. A conversion is one command, and the two interfaces are kept alike:

```bash
raster2dggs h3 --resolution 8 --overlay mass-preserve popcount.tif out/
vector2dggs s2 --resolution 16 --mode intersects -id parcel_id parcels.gpkg out/
```

[^1]: Formerly Manaaki Whenua – Landcare Research

# State of the field

The grid libraries themselves — H3, S2, A5, DGGAL, `rhealpixdggs` [@rhealpixdggs] — each implement one grid or family and operate on cells, not datasets. DGGRID [@dggrid] generates icosahedral grids and bins points to them from the command line, but does not read rasters. H3-Pandas [@h3pandas] and rHP-Pandas [@rhppandas] attach grid operations to in-memory GeoPandas [@geopandas] dataframes, one grid each; `xdggs` [@xdggs] does the same for Xarray, for data already on a grid; the `h3` extension for DuckDB [@h3duckdb; @duckdb] indexes inside SQL, for H3 only. Vgrid [@vgrid] is closest in scope: it covers more grids than our tools and converts rasters (binning or nearest neighbour) and vectors (by spatial predicate) from the command line, but assembles each result in memory, in one process, writes a single file, and offers fewer raster transformations.

We built rather than contributed because the need was not for more grid operations but for a route from geospatial data in any format to one joinable table, which set requirements the existing packages do not meet: inputs larger than memory; output partitioned so that later joins are also out-of-core; transfer rules including exact area weighting; and one interface across grid systems. The first two are architectural properties of a batch pipeline that cannot be added to an in-memory library. Every grid computation is delegated to the upstream library, and reading, geometry and area-weighted extraction to GDAL, Shapely [@shapely] and `exactextract` [@exactextract].

# Software design

**A command-line tool that writes Parquet.** Target inputs do not fit in memory, the consumers of the output do not share a language, and conversions run as pipeline steps on shared compute. A CLI whose product is a file serves all three.

**Common output format for raster and vector.** A raster is a grid of values and a vector layer is a set of shapes with attributes, but on a DGGS both reduce to rows of cell identifier and attributes (\autoref{fig:indexing}, \autoref{tbl:raster}, \autoref{tbl:vector}), so a join between a raster-derived and a vector-derived dataset is trivial. We chose Parquet [@parquet] because it is columnar: by Tobler's first law of geography, near things are more alike than distant ones [@tobler1970], so cells stored near one another carry similar values and their identifiers share most of their digits, which encodes and compresses well (\autoref{fig:size}) and lets analytical engines read partitions selectively. Both tools exploit this deliberately: each partition is written sorted by cell identifier, and that column is delta-encoded with Parquet's standard encodings, so consecutive identifiers cost a few bits each. The identifier column would otherwise dominate the file; sorted and delta-encoded it almost vanishes, and a categorical raster's output can be smaller than the compressed raster itself.

![Raster and vector inputs become cells. Top: Land Information New Zealand (LINZ) 0.3 m aerial imagery (CC BY 4.0) indexed by `raster2dggs` to H3 resolutions 12 and 13, each cell coloured by the mean of its pixels. Bottom: Land Cover Database (LCDB) v5.0 polygons for Rangatira, Chatham Islands (CC BY 4.0, BSI) indexed by `vector2dggs` to H3 resolutions 10 and 12, each cell carrying its feature's class. Finer resolutions follow the source more closely but require more rows.\label{fig:indexing}](figures/indexing-example.png){ width=100% }

**Spatially partitioned by parent cell.** Output is hive-partitioned on each cell's ancestor at a coarser resolution, keeping nearby cells together on disk, so two datasets indexed with the same parent resolution can be joined one partition at a time. Cell geometry is optional (GeoParquet [@geoparquet]) because it can be recomputed from the identifier, and compaction recursively replaces complete sets of sibling cells with their parent losslessly. The resolution 12 raster output in \autoref{fig:indexing} is laid out by its resolution 6 ancestors:

```
out/
  h3_06=86bb2b60fffffff/part.0.parquet
  h3_06=86bb2b657ffffff/part.1.parquet
  h3_06=86bb2b677ffffff/part.2.parquet
```

| `h3_12` | `h3_06` | `band_1` | `band_2` | `band_3` |
|--------------------|--------------------|---------|---------|---------|
| `8cbb2b609b693ff` | `86bb2b60fffffff` | 87 | 111 | 88 |
| `8cbb2b609b699ff` | `86bb2b60fffffff` | 91 | 116 | 92 |
| `8cbb2b609b69bff` | `86bb2b60fffffff` | 85 | 109 | 88 |
| `8cbb2b609b6d1ff` | `86bb2b60fffffff` | 76 | 101 | 85 |
| `8cbb2b609b6d3ff` | `86bb2b60fffffff` | 93 | 116 | 93 |

: The first rows of `raster2dggs` output for the resolution 12 imagery in \autoref{fig:indexing}: cell identifier, partition key (the cell's ancestor six levels coarser) and each band's mean over the cell's pixels.\label{tbl:raster}

| `h3_12` | `h3_06` | `LCDB_UID` | `Name_2018` |
|--------------------|--------------------|------------------|----------------------|
| `8cd46b2f00861ff` | `86d46b2f7ffffff` | lcdb3000001600 | Gravel or Rock |
| `8cd46b2f00865ff` | `86d46b2f7ffffff` | lcdb3000001600 | Gravel or Rock |
| `8cd46b2f00869ff` | `86d46b2f7ffffff` | lcdb3000001600 | Gravel or Rock |
| `8cd46b2f0086dff` | `86d46b2f7ffffff` | lcdb3000001600 | Gravel or Rock |
| `8cd46b2f00901ff` | `86d46b2f7ffffff` | lcdb3000001600 | Gravel or Rock |

: The first rows of `vector2dggs` output for the resolution 12 land-cover polygons in \autoref{fig:indexing} (8,428 rows): the identifier and attributes of the feature containing each cell.\label{tbl:vector}

![Size on disk of three inputs and their outputs by encoding (logarithmic axis): the imagery and polygons of \autoref{fig:indexing}, and ESA WorldCover 10 m land cover over the same island (categorical, indexed with `--overlay mode`), with the S2 DGGS at level 19 with uint64 cell IDs, partitioned at level 10. Solid bars are as written, pale bars compacted: compaction cuts the categorical table to a tenth of its rows and the polygons to a quarter, and leaves the imagery, whose sibling cells differ, largely unchanged.\label{fig:size}](figures/output-size.png){ width=100% }

**A single interface for all grid systems.** Each grid is described by a small specification from which its subcommand is generated, and implemented by an indexer class behind a common interface, loaded lazily from an optional dependency. Users need not learn each DGGS API (\autoref{fig:grids}), and additional grids can be added with a subclass.

![One argument selects the grid: the same raster window (top, `raster2dggs`) and polygons (middle, `vector2dggs`) on H3, rHEALPix, A5, S2 and Geohash, with nothing else in either command changed. Bottom: the vector output compacted, which exposes each grid's hierarchy; Geohash's aperture-32 hierarchy has no complete sibling set at this scale, so nothing compacts.\label{fig:grids}](figures/grids-example.png){ width=100% }

**Two packages.** Raster and vector conversion share the conventions above but otherwise differ in terms of dependency stacks and algorithms. Separate packages can be released independently.

**Work units sized to the problem.** Both tools parallelise over work units they size themselves. `raster2dggs` reads windows aligned to the raster's internal blocks, writes each window's cells to a temporary partitioned store, and merges cells split across windows with Dask [@dask] dataframes; `vector2dggs` bisects large polygons, stages features into chunks sized by their estimated cell count, indexes them in parallel, and removes any duplicates due to bisection.

**Transfer rules are explicit.** `raster2dggs` offers three strategies (\autoref{fig:transfer}): assigning each pixel centre to its cell; exact area-weighted overlay, which conserves totals for count-like variables and reports class fractions for categorical ones; and resampling at cell centres. `vector2dggs` fills each polygon with cells carrying the feature's attributes under one of three nested containment modes: cell centre inside, any intersection, or wholly within. Both tools handle geometries that cross the antimeridian [@antimeridian].

![The three raster-to-cell transfer strategies in `raster2dggs`, at a mask boundary in the LINZ 10 m national satellite mosaic (CC BY 4.0). Masked and nodata pixels are excluded in every strategy: from the aggregate, from the area weighting, and from the interpolation kernel, which is renormalised over its valid pixels. Cells with no valid pixels are omitted by default, or written with the source nodata value or a chosen stand-in under `--nodata emit`.\label{fig:transfer}](figures/raster-transfer-strategies.png){ width=90% }

# Research impact statement

The tools were built for land-use mapping and have been used for it in published work. @law2024 indexed 48 raster and vector layers to H3 with them and integrated the results into a land-use map of Northland, New Zealand, a workflow first presented at FOSS4G SotM Oceania 2023 [@law2023talk]; that paper received one of the journal's Best Paper Awards for 2024 [@bedprize] and has six Crossref citations to November 2026. The same approach at national scale underlies the New Zealand Land Use and Management (NZLUM) map published in 2026 [@nzlum], whose inputs were again indexed with these tools and which is itself on the H3 grid.

Both tools have been developed publicly on GitHub since March 2023 (LGPL-3.0-or-later), each with more than 40 tagged releases, about 400 commits from three contributors, and tests run in continuous integration; PyPI recorded about 4,400 downloads of `raster2dggs` and 5,700 of `vector2dggs`, excluding mirrors, between April and November 2026. Issues have been opened by people outside the BSI, including the authors of two wrapped grid libraries (A5 and DGGAL); `raster2dggs` is listed in A5's ecosystem documentation, and the University of Tartu's DGGS workshop at FOSS4G 2025 names the pair as tools for converting data into DGGS [@foss4g2025workshop]. Developing them has produced bug reports and feature requests for `exactextract`, A5 and DGGAL, and a line-tracing contribution merged into H3-Pandas.

# AI usage disclosure

Both tools were conceived, designed and first developed, from March 2023, before any generative AI assistance; that began in May 2026 for `raster2dggs` and July 2026 for `vector2dggs`. Since then Anthropic's Claude models (Sonnet 4.6, Sonnet 5, Opus 5, Opus 5.5, Fable 5 and Fable 5.1), used through Claude Code, have assisted with code generation, refactoring, test writing, performance investigation and documentation; commits made with this assistance carry a co-author trailer in the public history (66 of 398 commits in `raster2dggs`; 94 of 437 in `vector2dggs`). GitHub Copilot Autofix contributed two commits to `raster2dggs`.

# Acknowledgements

This work was supported in part by Strategic Science Investment Funding (SSIF) for Crown Research Institutes from the New Zealand Ministry of Business, Innovation and Employment's Science and Innovation Group, contract No. C09X1704. The funder had no involvement in the design or development of the software or in the writing of this paper.

# References
