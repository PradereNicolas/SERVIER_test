"""
Areas module init
"""

from enum import Enum

class AreaType(Enum):
    """Area type enum class
    """
    RAW = "raw"
    REFINED = "refined"
    OPTIMIZED = "optimized"
    BUSINESS = "business"

class DataFormat(Enum):
    """Data fromat enum class
    """
    CSV = "csv"
    JSON = "json"
    PICKLE = "pkl"

class Area:
    """Generic area class
    """
    def __init__(self, area_type: AreaType) -> None:
        """Initialize method
        """
        self._type = area_type
