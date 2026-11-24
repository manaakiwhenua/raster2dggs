| `h3_12` | `h3_06` | `band_1` | `band_2` | `band_3` |
|--------------------|--------------------|---------|---------|---------|
| `8cbb2b609b693ff` | `86bb2b60fffffff` | 87 | 111 | 88 |
| `8cbb2b609b699ff` | `86bb2b60fffffff` | 91 | 116 | 92 |
| `8cbb2b609b69bff` | `86bb2b60fffffff` | 85 | 109 | 88 |
| `8cbb2b609b6d1ff` | `86bb2b60fffffff` | 76 | 101 | 85 |
| `8cbb2b609b6d3ff` | `86bb2b60fffffff` | 93 | 116 | 93 |

: The first rows of `raster2dggs` output for the resolution 12 imagery in \autoref{fig:indexing} (4,901 rows): one row per cell, holding the cell identifier, the partition key (the cell's ancestor six levels coarser) and the mean of each band over the cell's pixels.\label{tbl:raster}

| `h3_12` | `h3_06` | `band_1` | `band_2` | `band_3` |
|------------------|------------------|----------------|----------------|----------------|
| `8cbb2b609b693ff` | `86bb2b60fffffff` | {60, 117, 87} | {84, 140, 111} | {70, 112, 88} |
| `8cbb2b609b699ff` | `86bb2b60fffffff` | {70, 130, 91} | {91, 152, 116} | {73, 120, 92} |
| `8cbb2b609b69bff` | `86bb2b60fffffff` | {56, 127, 85} | {75, 146, 109} | {63, 127, 88} |
| `8cbb2b609b6d1ff` | `86bb2b60fffffff` | {61, 89, 76} | {84, 115, 101} | {74, 96, 85} |
| `8cbb2b609b6d3ff` | `86bb2b60fffffff` | {63, 142, 93} | {80, 158, 116} | {67, 145, 93} |

: The same run with `-a min,max,mean`: each band becomes a struct column holding every requested aggregate, in order.\label{tbl:raster-agg}

| `h3_12` | `h3_06` | `band_1` |
|------------------|------------------|--------------------------------------------|
| `8cd46b2f00901ff` | `86d46b2f7ffffff` | {classes: [10, 80], fractions: [0.746, 0.254]} |
| `8cd46b2f00905ff` | `86d46b2f7ffffff` | {classes: [10, 30], fractions: [0.509, 0.491]} |
| `8cd46b2f00907ff` | `86d46b2f7ffffff` | {classes: [10, 30, 80], fractions: [0.141, 0.107, 0.752]} |
| `8cd46b2f00909ff` | `86d46b2f7ffffff` | {classes: [10, 80], fractions: [0.614, 0.386]} |
| `8cd46b2f0090bff` | `86d46b2f7ffffff` | {classes: [10, 80], fractions: [0.003, 0.997]} |

: ESA WorldCover 10 m land cover over the same island as \autoref{fig:indexing}, indexed by `raster2dggs` with `--overlay fractions` (23,861 rows, of which the first five with more than one class are shown): each cell holds the classes it overlaps and the fraction of its area under each, using WorldCover's class codes.\label{tbl:raster-frac}

| `h3_12` | `h3_06` | `LCDB_UID` | `Name_2018` |
|--------------------|--------------------|------------------|----------------------|
| `8cd46b2f00861ff` | `86d46b2f7ffffff` | lcdb3000001600 | Gravel or Rock |
| `8cd46b2f00865ff` | `86d46b2f7ffffff` | lcdb3000001600 | Gravel or Rock |
| `8cd46b2f00869ff` | `86d46b2f7ffffff` | lcdb3000001600 | Gravel or Rock |
| `8cd46b2f0086dff` | `86d46b2f7ffffff` | lcdb3000001600 | Gravel or Rock |
| `8cd46b2f00901ff` | `86d46b2f7ffffff` | lcdb3000001600 | Gravel or Rock |

: The first rows of `vector2dggs` output for the resolution 12 land-cover polygons in \autoref{fig:indexing} (8,428 rows): the identifier and attributes of the feature containing each cell.\label{tbl:vector}
