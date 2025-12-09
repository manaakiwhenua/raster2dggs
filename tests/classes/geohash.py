from classes.base import TestRunthrough
from data.datapaths import *
from raster2dggs.cli_factory import SPECS, make_command

geohash = make_command(next(s for s in SPECS if s.name == "geohash"))


class TestGeohash(TestRunthrough):
    """
    Sends the test data file through Geohash indexing using default parameters.
    """

    def test_geohash_run(self):
        try:
            geohash(
                [TEST_FILE_PATH, str(TEST_OUTPUT_PATH), "-r", "6"],
                standalone_mode=False,
            )

        except Exception:
            self.fail(f"TestGeohash.test_geohash_run: Geohash runthrough failed.")

    def test_geohash_run_geo_point(self):
        try:
            geohash(
                [TEST_FILE_PATH, str(TEST_OUTPUT_PATH), "-r", "6", "-g", "point"],
                standalone_mode=False,
            )

        except Exception:
            self.fail(
                f"TestGeohash.test_geohash_run_geo_point: Geohash runthrough failed."
            )

    def test_geohash_run_geo_polygon(self):
        try:
            geohash(
                [TEST_FILE_PATH, str(TEST_OUTPUT_PATH), "-r", "6", "-g", "polygon"],
                standalone_mode=False,
            )

        except Exception:
            self.fail(
                f"TestGeohash.test_geohash_run_geo_polygon: Geohash runthrough failed."
            )
