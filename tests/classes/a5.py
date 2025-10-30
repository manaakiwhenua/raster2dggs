from classes.base import TestRunthrough
from data.datapaths import *

from raster2dggs.a5 import a5


class TestA5(TestRunthrough):
    """
    Sends the test data file through A5 indexing using default parameters.
    """

    def test_a5_run(self):
        try:
            a5(
                [TEST_FILE_PATH, str(TEST_OUTPUT_PATH), "-r", "6"],
                standalone_mode=False,
            )

        except Exception:
            self.fail(f"TestA5.test_a5_run: A5 runthrough failed.")

    def test_a5_run_geo_point(self):
        try:
            a5(
                [TEST_FILE_PATH, str(TEST_OUTPUT_PATH), "-r", "6", "-g", "point"],
                standalone_mode=False,
            )

        except Exception:
            self.fail(f"TestA5.test_a5_run_geo_point: A5 runthrough failed.")

    def test_a5_run_geo_polygon(self):
        try:
            a5(
                [TEST_FILE_PATH, str(TEST_OUTPUT_PATH), "-r", "6", "-g", "polygon"],
                standalone_mode=False,
            )

        except Exception:
            self.fail(f"TestA5.test_a5_run_geo_polygon: A5 runthrough failed.")