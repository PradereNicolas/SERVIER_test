"""Refined clinical trial test module
"""

from areas.refined.jobs.clinical_trial import Job
from tests.utils import check_reject_comment

job = Job(is_test=True)

def test_refined_clinical_trial():
    """Refined clinical trial job test function
    """
    correct_df, reject_df = job.start()
    assert correct_df.shape[0] == 3
    assert reject_df.shape[0] == 5
    check_reject_comment(
        data_frame_record=reject_df[reject_df["clinical_trial_id"] == "NCT04189588"],
        reject_reason="Column date should not be empty"
    )
    check_reject_comment(
        data_frame_record=reject_df[reject_df["clinical_trial_id"] == "NCT04237090"],
        reject_reason="Column scientific_title should not be empty"
    )
    check_reject_comment(
        data_frame_record=reject_df[reject_df["clinical_trial_id"] == "NCT03490942"],
        reject_reason="Column journal should not be empty"
    )
    check_reject_comment(
        data_frame_record=reject_df[reject_df["clinical_trial_id"] == "NCT01967433"],
        reject_reason="Column date cannot be converted to <class 'numpy.datetime64'>"
    )
