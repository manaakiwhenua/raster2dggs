"""
@author: ndemaio
"""

import sys
import unittest

if __name__ == "__main__":
    testSuite = unittest.defaultTestLoader.discover(start_dir=".", top_level_dir=".")
    testRunner = unittest.TextTestRunner(stream=sys.stdout, verbosity=2, buffer=False)
    testRunner.run(testSuite)
