"""
@author: ndemaio
"""

import unittest

if __name__ == "__main__":
    testSuite = unittest.defaultTestLoader.discover(start_dir=".", top_level_dir=".")
    testRunner = unittest.TextTestRunner()
    testRunner.run(testSuite)
