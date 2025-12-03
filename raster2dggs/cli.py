import click

from raster2dggs import __version__
from raster2dggs.h3 import h3
from raster2dggs.rHP import rhp
from raster2dggs.geohash import geohash
from raster2dggs.maidenhead import maidenhead
from raster2dggs.s2 import s2
from raster2dggs.a5 import a5
from raster2dggs.isea9r import isea9r
from raster2dggs.isea7h import isea7h


@click.group()
@click.version_option(version=__version__)
def cli():
    pass


cli.add_command(h3)
cli.add_command(rhp)
cli.add_command(geohash)
cli.add_command(maidenhead)
cli.add_command(s2)
cli.add_command(a5)
cli.add_command(isea9r)
cli.add_command(isea7h)


def main():
    cli()
