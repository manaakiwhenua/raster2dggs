from classes.base import TestRunthrough
from data.datapaths import *

from raster2dggs.isea9r import isea9r


class TestISEA9R(TestRunthrough):
    """
    Sends the test data file through ISEA9R indexing using default parameters.
    """

    def test_isea9r_run(self):
        try:
            isea9r(
                [TEST_FILE_PATH, str(TEST_OUTPUT_PATH), "-r", "6"],
                standalone_mode=False,
            )

        except Exception:
            self.fail(f"TestISEA9R.test_isea9r_run: ISEA9R runthrough failed.")

    def test_isea9r_run_geo_point(self):
        try:
            isea9r(
                [TEST_FILE_PATH, str(TEST_OUTPUT_PATH), "-r", "6", "-g", "point"],
                standalone_mode=False,
            )

        except Exception:
            self.fail(
                f"TestISEA9R.test_isea9r_run_geo_point: ISEA9R runthrough failed."
            )

    def test_isea9r_run_geo_polygon(self):
        try:
            isea9r(
                [TEST_FILE_PATH, str(TEST_OUTPUT_PATH), "-r", "6", "-g", "polygon"],
                standalone_mode=False,
            )

        except Exception:
            self.fail(
                f"TestISEA9R.test_isea9r_run_geo_polygon: ISEA9R runthrough failed."
            )
