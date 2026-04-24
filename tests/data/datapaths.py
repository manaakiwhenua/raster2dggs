"""
@author: ndemaio
"""

from pathlib import Path

_DATA_DIR = Path(__file__).parent

TEST_FILE_PATH = str(_DATA_DIR / "se-island.tiff")
TEST_OUTPUT_PATH = _DATA_DIR / "output"
