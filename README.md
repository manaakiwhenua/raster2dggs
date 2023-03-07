# raster2dggs

Python-based CLI tool to index raster files to DGGS in parallel, writing out to Parquet.

Currently only supports H3 DGGS, and has other limitations since it has been developed for a specific internal use case. Contributions, suggestions, bug reports and strongly worded letters are all welcome.

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

```bash
rm -rf ./tests/data/output && poetry run raster2dggs --resolution 11 --compression zstd --resampling nearest s3://raster2dggs-test-data/Sen2_Test.tif
```

```


```