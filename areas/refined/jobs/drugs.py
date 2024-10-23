"""Drugs raw to refined job module
"""

from typing import Tuple, Optional
import pandas as pd

from areas import DefaultJob, AreaType
from areas.area import DataFormat

from areas.raw import DataType as RawDataType
from areas.refined import DataType as RefniedDataType
from areas.data_frame import Column

class Job(DefaultJob):
    """Drugs raw to refined job
    """
    def __init__(self, is_test: bool = False) -> None:
        """Init method

        Args:
            is_test (bool): Boolean value indicating if the job is \
instanciated in a test context
        """
        super().__init__(
            target_area=AreaType.REFINED,
            target_data_type=RefniedDataType.DRUGS,
            sources=[
                super().Source(
                    area=AreaType.RAW,
                    data_type=RawDataType.DRUGS,
                    data_format=DataFormat.CSV)
                ],
            target_schema=[
                Column(name="atccode", type_=str, required=True, is_functional_key=True),
                Column(name="drug", type_=str, required=True)
            ],
            is_test=is_test)

    def transform(self, raw_drugs_df: pd.DataFrame) -> Tuple[pd.DataFrame, Optional[pd.DataFrame]]:
        """Transform raw dataframe into refined dataframe

        Args:
            raw_drugs_df (pd.DataFrame): Raw dataframe

        Returns:
            Tuple[pd.DataFrame, Optional[pd.DataFrame]]: Refined dataframe and optionnaly the \
rejected dataframe
        """
        return raw_drugs_df.copy(), None
    