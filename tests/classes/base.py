"""
@author: ndemaio
"""

from unittest import *
from data.datapaths import *


class TestRunthrough(TestCase):
    """
    Parent class for the smoke tests. Handles temporary output files by
    overriding the built in setup and teardown methods from TestCase. Provides
    a new member function to recurse through nested output folders to empty
    them.
    """

    def setUp(self):
        TEST_OUTPUT_PATH.mkdir(exist_ok=True)

    def tearDown(self):
        if TEST_OUTPUT_PATH.exists():
            self.clearOutFolder(TEST_OUTPUT_PATH)
            TEST_OUTPUT_PATH.rmdir()

    def clearOutFolder(self, folder):
        for child in folder.iterdir():
            if child.is_dir():
                self.clearOutFolder(child)
                child.rmdir()
            else:
                child.unlink()
