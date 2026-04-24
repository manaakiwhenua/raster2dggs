"""
@author: ndemaio
"""

import sys
import unittest
from pathlib import Path

if __name__ == "__main__":
    tests_dir = str(Path(__file__).parent)
    if tests_dir not in sys.path:
        sys.path.insert(0, tests_dir)
    testSuite = unittest.defaultTestLoader.discover(start_dir=tests_dir, top_level_dir=tests_dir)
    testRunner = unittest.TextTestRunner(stream=sys.stdout, verbosity=2, buffer=False)
    testRunner.run(testSuite)
