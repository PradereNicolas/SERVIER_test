"""Refined drugs test module
"""

from areas.refined.jobs.drugs import Job
from tests.utils import check_reject_comment

job = Job(is_test=True)

def test_refined_drugs():
    """Refined drugs job test function
    """
    correct_df, reject_df = job.start()
    assert correct_df.shape[0] == 5
    assert reject_df.shape[0] == 2
    check_reject_comment(
        data_frame_record=reject_df[reject_df["atccode"] == "6302001"],
        reject_reason="Column drug should not be empty"
    )
    check_reject_comment(
        data_frame_record=reject_df[reject_df["drug"] == "BETAMETHASONE"],
        reject_reason="Column atccode should not be empty"
    )
