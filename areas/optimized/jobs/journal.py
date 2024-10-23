"""Journal refined to optimized job module
"""

from typing import Tuple, Optional
import pandas as pd

from areas import DefaultJob, AreaType
from areas.area import DataFormat

from areas.refined import DataType as RefniedDataType
from areas.optimized import DataType as OptimizedDataType
from areas.data_frame import Column

class Job(DefaultJob):
    """Journal raw to refined job
    """
    def __init__(self, is_test: bool = False) -> None:
        """Init method

        Args:
            is_test (bool): Boolean value indicating if the job is \
instanciated in a test context
        """
        super().__init__(
            target_area=AreaType.OPTIMIZED,
            target_data_type=OptimizedDataType.JOURNAL,
            sources=[
                super().Source(
                    area=AreaType.REFINED,
                    data_type=RefniedDataType.JOURNAL,
                    data_format=DataFormat.PICKLE)
                ],
            target_schema=[
                Column(name="journal_id", type_=str, required=True, is_functional_key=True),
                Column(name="name", type_=str, required=True)
            ],
            is_test=is_test)

    def transform(
        self,
        refined_journal_df: pd.DataFrame) -> Tuple[pd.DataFrame, Optional[pd.DataFrame]]:
        """Create refined journal dataframe

        Args:
            refined_journal_df (pd.DataFrame): Refined journal datafram

        Returns:
            Tuple[pd.DataFrame, Optional[pd.DataFrame]]: Refined dataframe and optionnaly the \
rejected dataframe
        """

        return refined_journal_df.copy(), None
    