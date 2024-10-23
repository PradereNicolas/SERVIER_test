"""
Optimized area module init
"""

from enum import Enum
from typing import Optional
import importlib

from areas import Area, AreaType

AREA_TYPE = AreaType.OPTIMIZED

class DataType(Enum):
    """Optimized data type enum
    """
    PUBLICATION = "publication"
    DRUGS = "drugs"
    JOURNAL = "journal"

class OptimizedArea(Area):
    """Optimized area class
    """
    def __init__(self) -> None:
        """Initialiser for optimized area
        """
        super().__init__(area_type=AREA_TYPE)

    def process_data(self, data_types: Optional[list[DataType]] = None) -> None:
        """Process data
        """
        if data_types is None:
            data_types = DataType
        for data_type in data_types:
            try:
                job_module = importlib.import_module(
                    name=f"areas.optimized.jobs.{data_type.value}",
                    package=None)
            except ModuleNotFoundError:
                print(f"Module {data_type} job has not been created")
                continue
            job = job_module.Job()
            print(f"Starting job {data_type}")
            job.start()
