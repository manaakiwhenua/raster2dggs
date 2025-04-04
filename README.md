# raster2dggs

[![pypi](https://img.shields.io/pypi/v/raster2dggs?label=raster2ddgs)](https://pypi.org/project/raster2dggs/)

Python-based CLI tool to index raster files to DGGS in parallel, writing out to Parquet.

Currently this supports the H3 and rHEALPix DGGSs. Contributions (particularly for additional DGGSs), suggestions, bug reports and strongly worded letters are all welcome.

![Example use case for raster2dggs, showing how an input raster can be indexed at different DGGS resolutions, while retaining information in separate, named bands](docs/imgs/raster2dggs-example.png "Example use case for raster2dggs, showing how an input raster can be indexed at different H3 resolutions, while retaining information in separate, named bands")

## Installation

`pip install raster2dggs`

## Usage

```
raster2dggs --help                                                                                                  [17:54:54]
Usage: raster2dggs [OPTIONS] COMMAND [ARGS]...

Options:
  --version  Show the version and exit.
  --help     Show this message and exit.

Commands:
  h3   Ingest a raster image and index it to the H3 DGGS.
  rhp  Ingest a raster image and index it to the rHEALPix DGGS.
```

```
raster2dggs h3 --help

Usage: raster2dggs h3 [OPTIONS] RASTER_INPUT OUTPUT_DIRECTORY

  Ingest a raster image and index it to the H3 DGGS.

  RASTER_INPUT is the path to input raster data; prepend with protocol like
  s3:// or hdfs:// for remote data. OUTPUT_DIRECTORY should be a directory,
  not a file, as it will be the write location for an Apache Parquet data
  store, with partitions equivalent to parent cells of target cells at a fixed
  offset. However, this can also be remote (use the appropriate prefix, e.g.
  s3://).

Options:
  -v, --verbosity LVL             Either CRITICAL, ERROR, WARNING, INFO or
                                  DEBUG  [default: INFO]
  -r, --resolution [0|1|2|3|4|5|6|7|8|9|10|11|12|13|14|15]
                                  H3 resolution to index  [required]
  -pr, --parent_res [0|1|2|3|4|5|6|7|8|9|10|11|12|13|14|15]
                                  H3 Parent resolution to index and aggregate
                                  to. Defaults to resolution - 6
  -u, --upscale INTEGER           Upscaling factor, used to upsample input
                                  data on the fly; useful when the raster
                                  resolution is lower than the target DGGS
                                  resolution. Default (1) applies no
                                  upscaling. The resampling method controls
                                  interpolation.  [default: 1]
  -c, --compression [snappy|gzip|zstd]
                                  Name of the compression to use when writing
                                  to Parquet.  [default: snappy]
  -t, --threads INTEGER           Number of threads to use when running in
                                  parallel. The default is determined based
                                  dynamically as the total number of available
                                  cores, minus one.  [default: 11]
  -a, --aggfunc [count|mean|sum|prod|std|var|min|max|median|mode]
                                  Numpy aggregate function to apply when
                                  aggregating cell values after DGGS indexing,
                                  in case of multiple pixels mapping to the
                                  same DGGS cell.  [default: mean]
  -d, --decimals INTEGER          Number of decimal places to round values
                                  when aggregating. Use 0 for integer output.
                                  [default: 1]
  -o, --overwrite
  --warp_mem_limit INTEGER        Input raster may be warped to EPSG:4326 if
                                  it is not already in this CRS. This setting
                                  specifies the warp operation's memory limit
                                  in MB.  [default: 12000]
  --resampling [nearest|bilinear|cubic|cubic_spline|lanczos|average|mode|gauss|max|min|med|q1|q3|sum|rms]
                                  Input raster may be warped to EPSG:4326 if
                                  it is not already in this CRS. Or, if the
                                  upscale parameter is greater than 1, there
                                  is a need to resample. This setting
                                  specifies this resampling algorithm.
                                  [default: average]
  --tempdir PATH                  Temporary data is created during the
                                  execution of this program. This parameter
                                  allows you to control where this data will
                                  be written.
  --version                       Show the version and exit.
  --help                          Show this message and exit.
```

## Visualising output

Output is in the Apache Parquet format, a directory with one file per partition. Partitions are based on parent cell IDs, with the parent resolution determined as an offset from the target DGGS resolution.

For a quick view of your output, you can read Apache Parquet with pandas, and then use h3-pandas and geopandas to convert this into a GeoPackage for visualisation in a desktop GIS, such as QGIS. The Apache Parquet output is indexed by the DGGS column, so it should be ready for association with other data prepared in the same DGGS.

```python
>>> import pandas as pd
>>> import h3pandas
>>> o = pd.read_parquet('./tests/data/output/9/Sen2_Test')
>>> o
band             B02  B03  B04  B05  B06  B07  B08  B8A  B11  B12
h3_09                                                            
89bb0981003ffff    9   27   16   62  175  197  228  247  102   36
89bb0981007ffff   10   30   17   66  185  212  238  261  113   40
89bb098100bffff   10   26   15   60  169  190  228  241  103   37
89bb098100fffff   11   29   17   66  181  203  243  257  109   39
89bb0981013ffff    8   26   16   58  172  199  220  244   98   34
...              ...  ...  ...  ...  ...  ...  ...  ...  ...  ...
89bb0d6eea7ffff   10   18   15   41  106  120  140  146  102   47
89bb0d6eeabffff   12   19   15   39   95  107  125  131   84   39
89bb0d6eeafffff   12   21   17   43  101  115  134  141  111   51
89bb0d6eeb7ffff   10   20   14   45  120  137  160  165  111   48
89bb0d6eebbffff   15   28   20   56  146  166  198  202  108   47

[5656 rows x 10 columns]
>>> o.h3.h3_to_geo_boundary().to_file('~/Downloads/Sen2_Test_h3-9.gpkg', driver='GPKG')
```

For rHEALPix DGGS output, you can use [`rHP-Pandas`](https://github.com/manaakiwhenua/rHP-Pandas):

```python
>>> import pandas as pd
>>> import rhppandas
>>> o = pd.read_parquet('./tests/data/output/11/Sen2_Test_rhp')
>>> o
band          B02  B03  B04  B05  B06  B07  B08  B8A  B11  B12
rhp_11                                                        
R88723652267   11   31   16   65  191  217  263  274   99   36
R88723652268   11   30   15   66  190  214  258  269   96   34
R88723652276   11   27   17   66  179  203  240  255   98   36
R88723652277   13   30   19   68  179  204  246  260  108   41
R88723652278   12   29   20   66  176  199  243  255  110   43
...           ...  ...  ...  ...  ...  ...  ...  ...  ...  ...
R88727068804   22   39   41   81  151  167  182  203  166   84
R88727068805   22   40   42   81  150  166  185  203  167   85
R88727068806   23   41   43   83  156  175  188  211  164   83
R88727068807   23   41   42   82  154  171  186  207  164   83
R88727068808   22   39   43   80  146  163  177  198  165   83

[223104 rows x 10 columns]
>>> o.rhp.rhp_to_geo_boundary().to_file('~/Downloads/Sen2_Test_rhp-11.gpkg', driver='GPKG')
```

## Installation

PyPi:

```bash
pip install raster2dggs
```

Conda environment:

```yaml
name: raster2dggs
channels:
  - conda-forge
channel_priority: strict
dependencies:
  - python>=3.11,<3.12
  - pip=23.1.*
  - gdal>=3.8.5
  - pyproj=3.6.*
  - pip:
    - raster2dggs>=0.2.6
```

<!-- TODO: package raster2dggs and make available on Conda without pip -->

### For development

In brief, to get started:

- Install [Poetry](https://python-poetry.org/docs/basic-usage/)
- Install [GDAL](https://gdal.org/)
    - If you're on Windows, `pip install gdal` may be necessary before running the subsequent commands.
    - On Linux, install GDAL 3.6+ according to your platform-specific instructions, including development headers, i.e. `libgdal-dev`.
- Create the virtual environment with `poetry init`. This will install necessary dependencies.
- Subsequently, the virtual environment can be re-activated with `poetry shell`.

If you run `poetry install`, the CLI tool will be aliased so you can simply use `raster2dggs` rather than `poetry run raster2dggs`, which is the alternative if you do not `poetry install`.

#### Code formatting

[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

Please run `black .` before committing.

#### Testing

Two sample files have been uploaded to an S3 bucket with `s3:GetObject` public permission.

- `s3://raster2dggs-test-data/Sen2_Test.tif` (sample Sentinel 2 imagery, 10 bands, rectangular, Int16, LZW compression, ~10x10m pixels, 68.6 MB)
- `s3://raster2dggs-test-data/TestDEM.tif` (sample LiDAR-derived DEM, 1 band, irregular shape with null data, Float32, uncompressed, 10x10m pixels, 183.5 MB)

You may use these for testing. However you can also test with local files too, which will be faster.

## Example commands

```bash
raster2dggs h3 --resolution 11 -d 0 s3://raster2dggs-test-data/Sen2_Test.tif ./tests/data/output/11/Sen2_Test
```

```bash
raster2dggs rhp --resolution 11 -d 0 s3://raster2dggs-test-data/Sen2_Test.tif ./tests/data/output/11/Sen2_Test_rhp
```

```bash
raster2dggs h3 --resolution 13 --compression zstd --resampling nearest -a median -d 1 -u 2 s3://raster2dggs-test-data/TestDEM.tif ./tests/data/output/13/TestDEM
```

## Citation

```bibtex
@software{raster2dggs,
  title={{raster2dggs}},
  author={Ardo, James and Law, Richard},
  url={https://github.com/manaakiwhenua/raster2dggs},
  version={0.2.7},
  date={2024-06-12}
}
```

APA/Harvard

> Ardo, J., & Law, R. (2024). raster2dggs (0.2.7) [Computer software]. https://github.com/manaakiwhenua/raster2dggs

[![manaakiwhenua-standards](https://github.com/manaakiwhenua/raster2dggs/workflows/manaakiwhenua-standards/badge.svg)](https://github.com/manaakiwhenua/manaakiwhenua-standards)
