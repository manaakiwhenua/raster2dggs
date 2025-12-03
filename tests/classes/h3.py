from classes.base import TestRunthrough
from data.datapaths import *

from raster2dggs.h3 import h3


class TestH3(TestRunthrough):
    """
    Sends the test data file through H3 indexing using default parameters.
    """

    def test_h3_run(self):
        try:
            h3(
                [TEST_FILE_PATH, str(TEST_OUTPUT_PATH), "-r", "6"],
                standalone_mode=False,
            )

        except Exception:
            self.fail(f"TestH3.test_h3_run: H3 runthrough failed.")

    def test_h3_run_geo_point(self):
        try:
            h3(
                [TEST_FILE_PATH, str(TEST_OUTPUT_PATH), "-r", "6", "-g", "point"],
                standalone_mode=False,
            )

        except Exception:
            self.fail(f"TestH3.test_h3_run_geo_point: H3 runthrough failed.")

    def test_h3_run_geo_polygon(self):
        try:
            h3(
                [TEST_FILE_PATH, str(TEST_OUTPUT_PATH), "-r", "6", "-g", "polygon"],
                standalone_mode=False,
            )

        except Exception:
            self.fail(f"TestH3.test_h3_run_geo_polygon: H3 runthrough failed.")
