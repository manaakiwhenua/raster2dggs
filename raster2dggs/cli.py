import click
from osgeo import gdal

from raster2dggs import __version__
from raster2dggs.cli_factory import SPECS, make_command

gdal.UseExceptions()


@click.group()
@click.version_option(version=__version__)
def cli():
    pass


for spec in SPECS:
    cli.add_command(make_command(spec))


def main():
    cli()
