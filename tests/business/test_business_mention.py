"""Business mention test module
"""

from areas.business.jobs.mention import Job
from tests.utils import check_reject_comment

job = Job(is_test=True)

def test_business_clinical_trial():
    """Business mention job test function
    """
    correct_df, reject_df = job.start()
    assert correct_df.shape[0] == 16
    assert reject_df.shape[0] == 1
    check_reject_comment(
        data_frame_record=reject_df.iloc[0],
        reject_reason="Column functional_id should not be empty"
    )
