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

A discrete global grid system (DGGS) divides the Earth's surface into a hierarchy of cells, each with a unique identifier [@sahr2003; @ogc2021]. Once two datasets are expressed on the same grid, combining them no longer needs geometry: it is a table join on the cell identifier. Getting data onto a common grid is made difficult because real geospatial datasets are often large, come as rasters (images, elevation models) or vectors (points, lines, polygons), and each grid system has its own software library.

`raster2dggs` [@raster2dggs] and `vector2dggs` [@vector2dggs] are a pair of command-line tools that abstract this conversion. Each reads any raster or vector source that GDAL [@gdal] can open and writes a Parquet [@parquet] dataset with one row per grid cell, at a resolution the user chooses. Both support H3 [@h3], S2 [@s2], rHEALPix [@gibb2016], A5 [@a5] and Geohash through the same interface; `raster2dggs` adds Maidenhead and thirteen further grids through DGGAL [@dggal]. Both process inputs larger than memory and in parallel, supporting use on a laptop or a cluster node.

# Statement of need

DGGS are promoted as a common reference frame for integrating heterogeneous spatial data [@purss2019], and the open-source grid libraries provide the necessary primitives: the cell containing a point, the cells covering a polygon, parent-child relationships, etc. A researcher must still read the source in pieces that fit in memory, reproject it, decide how a pixel or a polygon maps to cells, reconcile cells that straddle the pieces, handle the antimeridian, and write the result in an efficient form that other tools can read. That work must be repeated, with different APIs, for every grid system.

The tools were written at the Bioeconomy Science Institute (formerly Manaaki Whenua – Landcare Research) to provide abstraction. The motivation was to support integrating national-scale environmental layers (land cover, elevation, cadastral parcels) on a common grid; comparing grid systems, whose distortion properties differ [@kmoch2022]; producing cell-keyed feature tables for machine learning; and publishing DGGS-indexed versions of institutional datasets. The audience is researchers and data engineers who simply want the indexed table. A whole conversion is one command, and the two APIs are intentionally kept similar:

```bash
raster2dggs h3 --resolution 8 --overlay mass-preserve popcount.tif out/
vector2dggs s2 --resolution 16 --mode intersects -id parcel_id parcels.gpkg out/
```

# State of the field

The grid libraries themselves — H3, S2, A5, DGGAL, `rhealpixdggs` [@rhealpixdggs] — each implement one grid or family and operate on cells, not datasets. DGGRID [@dggrid] is a command-line program, but it generates icosahedral grids and bins point data to them; it does not read rasters. H3-Pandas [@h3pandas] and rHP-Pandas [@rhppandas] attach grid operations to in-memory GeoPandas [@geopandas] dataframes, one grid per package; `xdggs` [@xdggs] does the same for Xarray and concerns data already on a grid. The `h3` extension for DuckDB [@h3duckdb; @duckdb] indexes inside SQL but only concerns H3. Vgrid [@vgrid] is the closest in scope: it covers more grids than our tools and converts rasters (binning or nearest neighbour) and vectors (by spatial predicate) from the command line, but assembles each result in memory, in one process, and writes a single file and supports fewer transformations of raster data.

We built on these tools because the need was not for more grid operations but for a simple route from geospatial data in a variety of formats to a single joinable table, and that shaped requirements the existing packages did not meet in themselves: inputs larger than memory; output partitioned so that later joins are also out-of-core; transfer rules that include exact area weighting; and a single interface across different grid systems. The first two are architectural properties of a batch pipeline and could not be contributed to an in-memory library. The tools build upon the existing, mature ecosystem: every grid computation is delegated to the upstream library, and reading, geometry and area-weighted extraction to GDAL, Shapely [@shapely] and `exactextract` [@exactextract].

# Software design

**A command-line tool that writes Parquet.** Target inputs do not fit in memory, the consumers of the output do not use a single language, and conversions typically run as data transformation steps in pipelines on shared compute. A CLI whose product is a file serves all three requirements.

**Common output format for raster and vector.** A raster is a grid of values and a vector layer is a set of shapes with attributes, but on a DGGS both reduce to rows of cell identifier and attributes (\autoref{fig:raster}, \autoref{fig:vector}). We chose Parquet [@parquet] to hold that table because it is columnar. By Tobler's first law of geography, near things are more alike than distant ones [@tobler1970], so cells stored near one another carry similar or identical values, and the identifiers of neighbouring cells share most of their digits. Stored column by column, such data encodes and compresses very well, giving small files that analytical engines may read selectively (spatial partitions). A join between a raster-derived and a vector-derived dataset becomes trivial.

**Spatially partitioned by parent cell.** Output is hive-partitioned on each cell's ancestor at a coarser resolution, which keeps nearby cells together on disk. Two datasets indexed with the same parent resolution can be joined one partition at a time. Cell geometry is optional (GeoParquet [@geoparquet]) because it can always be recomputed from the identifier; and compaction can replace complete sets of sibling cells with their parent losslessly.

**A single interface for all grid systems.** Each grid is described by a small specification from which its subcommand is generated, and implemented by an indexer class behind a common interface, loaded lazily from an optional dependency. The value is in the abstraction: users do not need to learn each DGGS API (\autoref{fig:raster}, middle row). Support for a new grid system is achieved with a new indexer, by subclassing a common base class.

![`raster2dggs` applied to LINZ 0.3 m aerial imagery (CC BY 4.0): the same window indexed to H3 at two resolutions and to four other grids by changing one argument. The output table is shown in preview, and the disk size of the partitioned Parquet output.\label{fig:raster}](figures/raster-indexing-example.png){ width=100% }

![Land-cover polygons (LCDB v5.0, CC BY, LRIS) indexed to H3 cells by `vector2dggs`, giving the same form of table and partitioned output as \autoref{fig:raster}.\label{fig:vector}](figures/vector-indexing-example.png){ width=80% }

**Two packages.** Raster and vector conversion share the conventions above but little else: the dependency stacks differ, the algorithms differ, and separate packages can be released independently.

**Work units sized to the problem.** Both tools parallelise with a pool of worker processes over work units they size themselves, not a general task scheduler. `raster2dggs` reads windows aligned to the raster's internal blocks and writes each window's cells to a temporary partitioned store; a second stage, using Dask [@dask] dataframes, merges cells that were split across windows. `vector2dggs` recursively bisects large polygons, stages features into files sized by their estimated cell count, indexes them in parallel, and removes the duplicates that bisection creates. Although initially using Dask, the scheduler's chunks did not coincide with either unit, which caused repeated or oversized reads; explicit units are faster and easier to reason about.

**Transfer rules are explicit.** Development began with the simplest raster2dggs transfer rule and with attention on the form of the output; support for what the raster data can *mean* followed once that form was stable. `raster2dggs` now offers three strategies (\autoref{fig:transfer}): assigning each pixel centre to its cell; exact area-weighted overlay, which conserves totals for count-like variables and reports class fractions for categorical ones; and resampling at cell centres. `vector2dggs` fills each polygon with cells that carry the feature's attributes, under one of three nested containment modes: cell centre inside, any intersection, or wholly within. Both tools correctly address geometries that cross the antimeridian [@antimeridian].

![The three raster-to-cell transfer strategies in `raster2dggs`, at a mask boundary in the LINZ 10 m national satellite mosaic (CC BY 4.0).\label{fig:transfer}](figures/raster-transfer-strategies.png){ width=90% }

# Research impact statement

The tools were built for land-use mapping and have been used for this purpose in published work. @law2024 used them to index 48 disparate raster and vector layers to the H3 DGGS, and integrated the results into a land-use map of Northland, New Zealand; the tools and this workflow were first presented at FOSS4G SotM Oceania 2023 [@law2023talk]. The paper [@law2024] received one of the journal's Best Paper Awards for 2024 [@bedprize] and Crossref records six citations of it to November 2026. The same approach at national scale underlies the New Zealand Land Use and Management (NZLUM) map published by the Bioeconomy Science Institute in 2026 [@nzlum]: its inputs were again indexed to H3 with these tools for integration, and the published map is itself on the H3 grid.

Both tools have been developed publicly on GitHub since March 2023, under the LGPL-3.0-or-later licence, and are distributed on PyPI, which recorded about 4,400 downloads of `raster2dggs` and 5,700 of `vector2dggs`, excluding mirrors, between April and November 2026. Each has more than 40 tagged releases and about 400 commits from three contributors. Both have automated tests run in continuous integration (`raster2dggs`: 685 tests across Python 3.11–3.14) with formatting and lint checks. At the time of writing `raster2dggs` has 49 GitHub stars and 8 forks, and `vector2dggs` 18 and 3. Issues have been opened by people outside the institute, including the authors of two of the grid libraries the tools wrap (A5 and DGGAL). `raster2dggs` is listed in the A5 project's ecosystem documentation, and both tools are included in a community-curated list of DGGS software. The University of Tartu's Landscape Geoinformatics Lab's hands-on DGGS workshop for FOSS4G 2025 names the pair as existing tools for converting data into DGGS [@foss4g2025workshop]. Developing the tools has produced bug reports and feature requests for `exactextract`, A5 and DGGAL, and a line-tracing contribution merged into H3-Pandas.

# AI usage disclosure

Both tools were conceived, designed and first developed, from March 2023, before any generative AI assistance; that began in May 2026 for `raster2dggs` and July 2026 for `vector2dggs`. Since then Anthropic's Claude models (Sonnet 4.6, Sonnet 5, Opus 5, Opus 5.5, Fable 5 and Fable 5.1), used through Claude Code, have assisted with code generation, refactoring, test writing, performance investigation and documentation; commits made with this assistance carry a co-author trailer in the public history (66 of 398 commits in `raster2dggs`; 94 of 437 in `vector2dggs`). GitHub Copilot Autofix contributed two commits to `raster2dggs`.

# Acknowledgements

This work was supported in part by Strategic Science Investment Funding (SSIF) for Crown Research Institutes from the New Zealand Ministry of Business, Innovation and Employment's Science and Innovation Group, contract No. C09X1704. The funder had no involvement in the design or development of the software or in the writing of this paper.

# References
