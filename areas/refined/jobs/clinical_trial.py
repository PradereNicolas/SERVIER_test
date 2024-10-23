"""Clinical trials raw to refined job module
"""

from typing import Tuple, Optional
import numpy as np
import pandas as pd

from areas import DefaultJob, AreaType
from areas.utils import generate_functional_key
from areas.area import DataFormat

from areas.raw import DataType as RawDataType
from areas.refined import DataType as RefniedDataType
from areas.data_frame import Column

class Job(DefaultJob):
    """Clinical trials raw to refined job
    """
    def __init__(self, is_test: bool = False) -> None:
        """Init method

        Args:
            is_test (bool): Boolean value indicating if the job is \
instanciated in a test context
        """
        super().__init__(
            target_area=AreaType.REFINED,
            target_data_type=RefniedDataType.CLINICAL_TRIAL,
            sources=[
                super().Source(
                    area=AreaType.RAW,
                    data_type=RawDataType.CLINICAL_TRIAL,
                    data_format=DataFormat.CSV)
                ],
            target_schema=[
                Column(name="clinical_trial_id", type_=str, required=True),
                Column(name="scientific_title", type_=str, required=True),
                Column(name="date", type_=np.datetime64, required=True),
                Column(name="journal", type_=str, required=True)
            ],
            is_test=is_test)

    def transform(
        self,
        raw_clinical_trials_df: pd.DataFrame) -> Tuple[pd.DataFrame, Optional[pd.DataFrame]]:
        """Transform raw dataframe into refined dataframe

        Args:
            raw_clinical_trials_df (pd.DataFrame): Raw dataframe

        Returns:
            Tuple[pd.DataFrame, Optional[pd.DataFrame]]: Refined dataframe and optionnaly the \
rejected dataframe
        """
        refined_df = raw_clinical_trials_df.copy()
        refined_df.rename(columns={"id": "clinical_trial_id"}, inplace=True)
        return refined_df, None
    