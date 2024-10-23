"""Refined pubmed test module
"""

from areas.refined.jobs.pubmed import Job
from tests.utils import check_reject_comment

job = Job(is_test=True)

def test_refined_pubmed():
    """Refined pubmed job test function
    """
    correct_df, reject_df = job.start()
    assert correct_df.shape[0] == 6
    assert reject_df.shape[0] == 7
    check_reject_comment(
        data_frame_record=reject_df[reject_df["pubmed_id"] == "a"],
        reject_reason="Column pubmed_id cannot be converted to <class 'int'>"
    )
    check_reject_comment(
        data_frame_record=reject_df[reject_df["pubmed_id"] == "3"],
        reject_reason="Column title should not be empty"
    )
    check_reject_comment(
        data_frame_record=reject_df[reject_df["pubmed_id"] == "4"],
        reject_reason="Column date should not be empty"
    )
    check_reject_comment(
        data_frame_record=reject_df[reject_df["pubmed_id"] == "12"],
        reject_reason="Column journal should not be empty"
    )
