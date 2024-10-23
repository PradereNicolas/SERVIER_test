"""Refined journal test module
"""

from areas.refined.jobs.journal import Job
from tests.utils import check_reject_comment

job = Job(is_test=True)

def test_refined_journal():
    """Refined journal job test function
    """
    correct_df, reject_df = job.start()
    assert correct_df.shape[0] == 10
    assert reject_df.shape[0] == 1
    check_reject_comment(
        data_frame_record=reject_df.iloc[0],
        reject_reason="Column name should not be empty"
    )
