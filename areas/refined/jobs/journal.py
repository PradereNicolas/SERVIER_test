"""Journal raw to refined job module
"""

from typing import Tuple, Optional
import pandas as pd

from areas import DefaultJob, AreaType
from areas.utils import generate_functional_key
from areas.area import DataFormat

from areas.raw import DataType as RawDataType
from areas.refined import DataType as RefniedDataType
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
            target_area=AreaType.REFINED,
            target_data_type=RefniedDataType.JOURNAL,
            sources=[
                super().Source(
                    area=AreaType.REFINED,
                    data_type=RawDataType.CLINICAL_TRIAL,
                    data_format=DataFormat.PICKLE),
                super().Source(
                    area=AreaType.REFINED,
                    data_type=RawDataType.PUBMED,
                    data_format=DataFormat.PICKLE)
                ],
            target_schema=[
                Column(name="journal_id", type_=str, required=True, is_functional_key=True),
                Column(name="name", type_=str, required=True)
            ],
            is_test=is_test)

    def transform(
        self,
        refined_clinical_trial_df: pd.DataFrame,
        refined_pubmed_df: pd.DataFrame) -> Tuple[pd.DataFrame, Optional[pd.DataFrame]]:
        """Create refined journal dataframe

        Args:
            refined_clinical_trial_df (pd.DataFrame): Refined clinical trial dataframe
            refined_pubmed_df (pd.DataFrame): Refined clinical trial dataframe

        Returns:
            Tuple[pd.DataFrame, Optional[pd.DataFrame]]: Refined dataframe and optionnaly the \
rejected dataframe
        """
        journal_df = pd.concat(
            [refined_clinical_trial_df[["journal"]], refined_pubmed_df[["journal"]]])
        journal_df.rename(columns={"journal": "name"}, inplace=True)
        journal_df["journal_id"] = generate_functional_key(data_frame=journal_df, columns=["name"])
        journal_df.drop_duplicates(subset="journal_id", inplace=True)
        return journal_df, None
    