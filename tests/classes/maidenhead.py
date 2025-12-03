from classes.base import TestRunthrough
from data.datapaths import *

from raster2dggs.maidenhead import maidenhead


class TestMaidenhead(TestRunthrough):
    """
    Sends the test data file through Maidenhead indexing using default parameters.
    """

    def test_maidenhead_run(self):
        try:
            maidenhead(
                [TEST_FILE_PATH, str(TEST_OUTPUT_PATH), "-r", "3"],
                standalone_mode=False,
            )

        except Exception as e:
            self.fail(
                f"TestMaidenhead.test_maidenhead_run: Maidenhead runthrough failed."
            )

    def test_maidenhead_run_geo_point(self):
        try:
            maidenhead(
                [TEST_FILE_PATH, str(TEST_OUTPUT_PATH), "-r", "6", "-g", "point"],
                standalone_mode=False,
            )

        except Exception:
            self.fail(
                f"TestMaidenhead.test_maidenhead_run_geo_point: Maidenhead runthrough failed."
            )

    def test_maidenhead_run_geo_polygon(self):
        try:
            maidenhead(
                [TEST_FILE_PATH, str(TEST_OUTPUT_PATH), "-r", "6", "-g", "polygon"],
                standalone_mode=False,
            )

        except Exception:
            self.fail(
                f"TestMaidenhead.test_maidenhead_run_geo_polygon: Maidenhead runthrough failed."
            )
