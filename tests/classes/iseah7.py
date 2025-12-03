from classes.base import TestRunthrough
from data.datapaths import *

from raster2dggs.isea7h import isea7h


class TestISEA7H(TestRunthrough):
    """
    Sends the test data file through ISEA7H indexing using default parameters.
    """

    def test_isea7h_run(self):
        try:
            isea7h(
                [TEST_FILE_PATH, str(TEST_OUTPUT_PATH), "-r", "6"],
                standalone_mode=False,
            )

        except Exception:
            self.fail(f"TestISEA7H.test_isea7h_run: ISEA7H runthrough failed.")

    def test_isea7h_run_geo_point(self):
        try:
            isea7h(
                [TEST_FILE_PATH, str(TEST_OUTPUT_PATH), "-r", "6", "-g", "point"],
                standalone_mode=False,
            )

        except Exception:
            self.fail(
                f"TestISEA7H.test_isea7h_run_geo_point: ISEA7H runthrough failed."
            )

    def test_isea7h_run_geo_polygon(self):
        try:
            isea7h(
                [TEST_FILE_PATH, str(TEST_OUTPUT_PATH), "-r", "6", "-g", "polygon"],
                standalone_mode=False,
            )

        except Exception:
            self.fail(
                f"TestISEA7H.test_isea7h_run_geo_polygon: ISEA7H runthrough failed."
            )
