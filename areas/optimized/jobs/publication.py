"""Clinical trials refined to optimized job module
"""

from typing import Tuple, Optional
import numpy as np
import pandas as pd

from areas import DefaultJob
from areas.utils import generate_functional_key
from areas.area import DataFormat, AreaType

from areas.refined import DataType as RefniedDataType
from areas.optimized import DataType as OptimizedDataType
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
            target_area=AreaType.OPTIMIZED,
            target_data_type=OptimizedDataType.PUBLICATION,
            sources=[
                super().Source(
                    area=AreaType.REFINED,
                    data_type=RefniedDataType.CLINICAL_TRIAL,
                    data_format=DataFormat.PICKLE),
                super().Source(
                    area=AreaType.REFINED,
                    data_type=RefniedDataType.PUBMED,
                    data_format=DataFormat.PICKLE),
                super().Source(
                    area=AreaType.OPTIMIZED,
                    data_type=OptimizedDataType.JOURNAL,
                    data_format=DataFormat.PICKLE)
                ],
            target_schema=[
                Column(name="title", type_=str, required=True),
                Column(name="date", type_=np.datetime64, required=True),
                Column(name="journal_id", type_=str, required=True),
                Column(name="publication_type", type_=str, required=True),
                Column(name="functional_id", type_=str, required=True)
            ],
            is_test=is_test)

    def transform(
        self,
        refined_clinical_trials_df: pd.DataFrame,
        refined_pubmed_df: pd.DataFrame,
        refined_journal_df: pd.DataFrame) -> Tuple[pd.DataFrame, Optional[pd.DataFrame]]:
        """Transform raw dataframe into refined dataframe

        Args:
            refined_clinical_trials_df (pd.DataFrame): Refined clinical trial dataframe
            refined_pubmed_df (pd.DataFrame): Refined clinical trial dataframe
            refined_journal_trials_df (pd.DataFrame): Refined journal dataframe

        Returns:
            Tuple[pd.DataFrame, Optional[pd.DataFrame]]: Refined dataframe and optionnaly the \
rejected dataframe
        """

        def check_journal_existence(data_frame: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
            """Check if journal exist and return with journal id

            Args:
                data_frame (pd.DataFrame): Refined dataframe to be checked

            Returns:
                Tuple[pd.DataFrame, pd.DataFrame]: Dataframe and rejected dataframe
            """
            optimized_df = data_frame.copy()
            optimized_df["journal_id"] = generate_functional_key(
                data_frame=optimized_df,
                columns=["journal"]
            )
            optimized_df = optimized_df.merge(
                refined_journal_df[["id", "journal_id"]].rename(columns={"id": "_id"}),
                on="journal_id",
                how="outer",
                indicator=True
            )
            optimized_rejected_df = optimized_df.query('_merge=="left_only"')
            optimized_rejected_df["reject_reason"] = "Journal not found"

            optimized_df = optimized_df.query('_merge=="both"')
            optimized_df = optimized_df.drop(columns=["journal_id"]) \
                .rename(columns={"_id": "journal_id"})
            return optimized_df, optimized_rejected_df

        refined_clinical_trials_df.rename(columns={
            "clinical_trial_id": "functional_id"
        }, inplace=True)
        optimized_clinical_trials_df, optimized_clinical_trials_rejected_df = check_journal_existence(
            data_frame=refined_clinical_trials_df
        )
        optimized_clinical_trials_df.rename(columns={"scientific_title": "title"}, inplace=True)
        optimized_clinical_trials_df.insert(0, "publication_type", RefniedDataType.CLINICAL_TRIAL.name)
        optimized_clinical_trials_rejected_df.insert(0, "publication_type", RefniedDataType.CLINICAL_TRIAL.name)

        refined_pubmed_df.rename(columns={
            "pubmed_id": "functional_id"
        }, inplace=True)
        optimized_pubmed_df, optimized_pubmed_rejected_df = check_journal_existence(
            data_frame=refined_pubmed_df
        )
        optimized_pubmed_df.insert(0, "publication_type", RefniedDataType.PUBMED.name)
        optimized_pubmed_rejected_df.insert(0, "publication_type", RefniedDataType.PUBMED.name)

        optimized_publication_df = pd.concat([
            optimized_clinical_trials_df.dropna(axis=1, how="all"),
            optimized_pubmed_df.dropna(axis=1, how="all")])
        optimized_publication_reject_df = pd.concat([
            optimized_clinical_trials_rejected_df.dropna(axis=1, how="all"),
            optimized_pubmed_rejected_df.dropna(axis=1, how="all")])

        return optimized_publication_df, optimized_publication_reject_df
