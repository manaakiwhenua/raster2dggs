# raster2dggs

Python-based CLI tool to index raster files to DGGS in parallel, writing out to Parquet.

Currently only supports H3 DGGS, and probably has other limitations since it has been developed for a specific internal use case, though it is intended as a general-purpose abstraction. Contributions, suggestions, bug reports and strongly worded letters are all welcome.

```
raster2dggs --help

Usage: raster2dggs [OPTIONS] RASTER_INPUT OUTPUT_DIRECTORY

  Ingest a raster image and index it to the H3 DGGS.

  RASTER_INPUT is the path to input raster data; prepend with protocol like
  s3:// or hdfs:// for remote data. OUTPUT_DIRECTORY should be a directory,
  not a file, as it will be the write location for an Apache Parquet data
  store, with partitions equivalent to parent cells of target cells at a fixed
  offset. However, this can also be remote (use the appropriate prefix, e.g.
  s3://).

Options:
  -r, --resolution [0|1|2|3|4|5|6|7|8|9|10|11|12|13|14|15]
                                  H3 resolution to index  [required]
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
                                  parallel  [default: 7]
  -a, --aggfunc [count|mean|sum|prod|std|var|min|max|median]
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
                                  it is not already in this CRS. This setting
                                  specifies the warp resampling algorithm.
                                  [default: average]
  --help                          Show this message and exit.
```

## Development 

Generally, follow the instructions for basic usage of [`poetry`](https://python-poetry.org/docs/basic-usage/).

In brief, to get started:

    - Create the virtual environment with `poetry init`. This will install necessary dependencies. However there is an external dependency on GDAL 3.6+ (including development headers, i.e. `libgdal-dev`).
    - Subsequently, the virtual environment can be re-activated with `poetry shell`.

### Testing

Two sample files have been uploaded to an S3 bucket with `s3:GetObject` public permission.

- `s3://raster2dggs-test-data/Sen2_Test.tif` (sample Sentinel 2 imagery, 10 bands, rectangular, Int16, LZW compression, ~10x10m pixels, 68.6 MB)
- `s3://raster2dggs-test-data/TestDEM.tif` (sample LiDAR-derived DEM, 1 band, irregular shape with null data, Float32, uncompressed, 10x10m pixels, 183.5 MB)

You may use these for testing. However you can also test with local files too, which will be faster.

### Example commands

```bash
poetry run raster2dggs --resolution 11 -d 0 s3://raster2dggs-test-data/Sen2_Test.tif ./tests/data/output/11/Sen2_Test
```

```
poetry run raster2dggs --resolution 13 --compression zstd --resampling nearest -a median -d 1 -u 2 s3://raster2dggs-test-data/TestDEM.tif ./tests/data/output/13/TestDEM
```