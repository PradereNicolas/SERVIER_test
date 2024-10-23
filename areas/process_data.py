"""Workflow process
"""

from .refined import RefinedArea
from .optimized import OptimizedArea
from .business import BusinessArea

def process_data(
    refined_area: RefinedArea,
    optimized_area: OptimizedArea,
    business_area: BusinessArea) -> None:
    """Process data from raw area to optimized area

    Args:
        refined_area (RefinedArea): Refined area
        optimized_area (OptimizedArea): Optimized area
    """
    refined_area.process_data()
    optimized_area.process_data()
    business_area.process_data()
