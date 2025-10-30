from classes.base import TestRunthrough
from data.datapaths import *

from raster2dggs.rHP import rhp


class TestRHP(TestRunthrough):
    """
    Sends the test data file through rHP indexing using default parameters.
    """

    def test_rhp_run(self):
        try:
            rhp(
                [TEST_FILE_PATH, str(TEST_OUTPUT_PATH), "-r", "6"],
                standalone_mode=False,
            )

        except Exception:
            self.fail(f"TestRHP.test_rhp_run: rHP runthrough failed.")

    def test_rhp_run_geo_point(self):
        try:
            rhp(
                [TEST_FILE_PATH, str(TEST_OUTPUT_PATH), "-r", "6", "-g", "point"],
                standalone_mode=False,
            )

        except Exception:
            self.fail(f"TestRHP.test_rhp_run_geo_point: rHP runthrough failed.")

    def test_rhp_run_geo_polygon(self):
        try:
            rhp(
                [TEST_FILE_PATH, str(TEST_OUTPUT_PATH), "-r", "6", "-g", "polygon"],
                standalone_mode=False,
            )

        except Exception:
            self.fail(f"TestRHP.test_rhp_run_geo_polygon: rHP runthrough failed.")