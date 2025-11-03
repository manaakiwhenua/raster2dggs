from classes.base import TestRunthrough
from data.datapaths import *

from raster2dggs.s2 import s2


class TestS2(TestRunthrough):
    """
    Sends the test data file through S2 indexing using default parameters.
    """

    def test_s2_run(self):
        try:
            s2(
                [TEST_FILE_PATH, str(TEST_OUTPUT_PATH), "-r", "6"],
                standalone_mode=False,
            )

        except Exception:
            self.fail(f"TestS2.test_s2_run: S2 runthrough failed.")

    def test_s2_run_geo_point(self):
        try:
            s2(
                [TEST_FILE_PATH, str(TEST_OUTPUT_PATH), "-r", "6", "-g", "point"],
                standalone_mode=False,
            )

        except Exception:
            self.fail(f"TestS2.test_s2_run_geo_point: S2 runthrough failed.")

    def test_s2_run_geo_polygon(self):
        try:
            s2(
                [TEST_FILE_PATH, str(TEST_OUTPUT_PATH), "-r", "6", "-g", "polygon"],
                standalone_mode=False,
            )

        except Exception:
            self.fail(f"TestS2.test_s2_run_geo_polygon: S2 runthrough failed.")