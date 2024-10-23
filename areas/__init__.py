"""Area module init
"""

from .area import Area, AreaType
from .job import DefaultJob
from .process_data import process_data

from .raw import RawArea
from .refined import RefinedArea
from .optimized import OptimizedArea
from .business import BusinessArea

RAW_AREA = RawArea()
REFINED_AREA = RefinedArea()
OPTIMIZED_AREA = OptimizedArea()
BUSINESS_AREA = BusinessArea()
