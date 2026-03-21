import sys
from pathlib import Path
_root = str(Path(__file__).resolve().parent)
if _root not in sys.path:
    sys.path.insert(0, _root)
from core.db_utils import *
from core.db_utils import _db_connect  # re-export private symbol used by legacy scripts
