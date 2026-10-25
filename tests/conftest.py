import sys
from pathlib import Path

# make the project folder importable from the tests
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
