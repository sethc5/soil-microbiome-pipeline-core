import sys
from pathlib import Path
_root = str(Path(__file__).resolve().parent)
if _root not in sys.path:
    sys.path.insert(0, _root)
from core.config_schema import *
