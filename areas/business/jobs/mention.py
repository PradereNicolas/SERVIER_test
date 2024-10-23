"""Mention refined to optimized job module
"""

from typing import Tuple, Optional
import pandas as pd

from areas import DefaultJob, AreaType
from areas.area import DataFormat

from areas.optimized import DataType as OptimizedDataType
from areas.business import DataType as BusinessDataType
from areas.data_frame import Column

class Job(DefaultJob):
    """Mention optimized to business job
    """
    def __init__(self, is_test: bool = False) -> None:
        """Init method

        Args:
            is_test (bool): Boolean value indicating if the job is \
instanciated in a test context
        """
        super().__init__(
            target_area=AreaType.BUSINESS,
            target_data_type=BusinessDataType.MENTION,
            sources=[
                super().Source(
                    area=AreaType.OPTIMIZED,
                    data_type=OptimizedDataType.DRUGS,
                    data_format=DataFormat.PICKLE),
                super().Source(
                    area=AreaType.OPTIMIZED,
                    data_type=OptimizedDataType.JOURNAL,
                    data_format=DataFormat.PICKLE),
                super().Source(
                    area=AreaType.OPTIMIZED,
                    data_type=OptimizedDataType.PUBLICATION,
                    data_format=DataFormat.PICKLE)
                ],
            target_schema=[
                Column(name="title", type_=str, required=True),
                Column(name="publication_type", type_=str, required=True),
                Column(name="publication_date", type_=str, required=True),
                Column(name="journal_name", type_=str, required=True),
                Column(name="drug", type_=str, required=True),
                Column(name="drug_id", type_=str, required=True),
                Column(name="publication_id", type_=str, required=True),
                Column(name="journal_id", type_=str, required=True),
                Column(name="functional_id", type_=str, required=True)
            ],
            is_test=is_test)

    def transform(
        self,
        optimized_drugs_df: pd.DataFrame,
        optimized_journal_df: pd.DataFrame,
        optimized_publication_df: pd.DataFrame) -> Tuple[pd.DataFrame, Optional[pd.DataFrame]]:
        """Create business drugs dataframe

        Args:
            optimized_drugs_df (pd.DataFrame): Optimized drugs dataframe
            optimized_journal_df (pd.DataFrame): Optimized journal dataframe
            optimized_publication_df (pd.DataFrame): Optimized publication dataframe

        Returns:
            Tuple[pd.DataFrame, Optional[pd.DataFrame]]: Refined dataframe and optionnaly the \
rejected dataframe
        """
        mentions_df = optimized_publication_df.copy()
        mentions_df.rename(columns={"id": "publication_id"}, inplace=True)
        mentions_df["mentions"] = mentions_df["title"].str.upper().str.split(" ")
        mentions_df = mentions_df.explode("mentions")

        mentions_df = mentions_df.merge(
            optimized_drugs_df,
            left_on="mentions",
            right_on="drug",
            how="right"
        )

        mentions_df.rename(columns={
            "id": "drug_id",
            "date": "publication_date"
            }, inplace=True)

        optimized_journal_df = optimized_journal_df[["id", "name"]].rename(columns={
            "id": "journal_id",
            "name": "journal_name"})

        mentions_df = mentions_df.merge(optimized_journal_df, on="journal_id", how="left")

        mentions_df.drop_duplicates(subset=["drug", "publication_id", "journal_id"], inplace=True)

        return mentions_df.copy(), None
    