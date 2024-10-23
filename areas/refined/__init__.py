"""
Refined area module init
"""

from enum import Enum
from typing import Optional
import importlib

from areas import Area, AreaType

AREA_TYPE = AreaType.REFINED

class DataType(Enum):
    """Refined data type enum
    """
    CLINICAL_TRIAL = "clinical_trial"
    DRUGS = "drugs"
    PUBMED = "pubmed"
    JOURNAL = "journal"

class RefinedArea(Area):
    """Refined area class
    """
    def __init__(self) -> None:
        """Initialiser for refined area
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
                    name=f"areas.refined.jobs.{data_type.value}",
                    package=None)
            except ModuleNotFoundError:
                print(f"Module {data_type} job has not been created")
                continue
            job = job_module.Job()
            print(f"Starting job {data_type}")
            job.start()
