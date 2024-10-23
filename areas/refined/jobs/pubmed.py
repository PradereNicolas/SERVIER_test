"""Pubmed raw to refined job module
"""

from typing import Tuple, Optional
import numpy as np
import pandas as pd

from areas import DefaultJob, AreaType
from areas.area import DataFormat

from areas.raw import DataType as RawDataType
from areas.refined import DataType as RefniedDataType
from areas.data_frame import Column

class Job(DefaultJob):
    """Pubmed raw to refined job
    """
    def __init__(self, is_test: bool = False) -> None:
        """Init method

        Args:
            is_test (bool): Boolean value indicating if the job is \
instanciated in a test context
        """
        super().__init__(
            target_area=AreaType.REFINED,
            target_data_type=RefniedDataType.PUBMED,
            sources=[
                super().Source(
                    area=AreaType.RAW,
                    data_type=RawDataType.PUBMED,
                    data_format=DataFormat.CSV),
                super().Source(
                    area=AreaType.RAW,
                    data_type=RawDataType.PUBMED,
                    data_format=DataFormat.JSON)
                ],
            target_schema=[
                Column(name="pubmed_id", type_=int, required=True, is_functional_key=True),
                Column(name="title", type_=str, required=True),
                Column(name="date", type_=np.datetime64, required=True),
                Column(name="journal", type_=str, required=True)
            ],
            is_test=is_test)

    def transform(
        self,
        raw_pubmed_csv_df: pd.DataFrame,
        raw_pubmed_json_df: pd.DataFrame) -> Tuple[pd.DataFrame, Optional[pd.DataFrame]]:
        """Transform raw dataframe into refined dataframe

        Args:
            raw_pubmed_csv_df (pd.DataFrame): Raw dataframe
            raw_pubmed_json_df (pd.DataFrame): Raw dataframe

        Returns:
            Tuple[pd.DataFrame, Optional[pd.DataFrame]]: Refined dataframe and optionnaly the \
rejected dataframe
        """
        refined_df = pd.concat([raw_pubmed_csv_df.copy(), raw_pubmed_json_df.copy()])
        refined_df.rename(columns={"id": "pubmed_id"}, inplace=True)
        return refined_df, None
    