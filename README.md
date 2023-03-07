# raster2dggs

## Development 

Generally, follow the instructions for basic usage of [`poetry`](https://python-poetry.org/docs/basic-usage/).

In brief, to get started:

    - Create the virtual environment with `poetry init`. This will install necessary dependencies. However there is an external dependency on GDAL 3.6+ (including development headers, i.e. `libgdal-dev`).
    - Subsequently, the virtual environment can be re-activated with `poetry shell`.

### Testing

```bash
rm -rf ./tests/data/output && poetry run raster2dggs --resolution 11 --compression zstd --resampling nearest ./tests/data/input/Sen2_Test.tif
```