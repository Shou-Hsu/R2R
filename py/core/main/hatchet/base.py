from hatchet_sdk import Hatchet

try:
    # r2r_hatchet = Hatchet()
    from unittest.mock import MagicMock
    r2r_hatchet = MagicMock()
    pass
except ImportError:
    r2r_hatchet = None
