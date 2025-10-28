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
