"""Optimized drugs test module
"""

from areas.optimized.jobs.drugs import Job
from tests.utils import check_reject_comment

job = Job(is_test=True)

def test_optimized_drugs():
    """Optimized drugs job test function
    """
    correct_df, reject_df = job.start()
    assert correct_df.shape[0] == 4
    assert reject_df.shape[0] == 4
    check_reject_comment(
        data_frame_record=reject_df[reject_df["atccode"] == "6302001"],
        reject_reason="Column drug should not be empty"
    )
    assert reject_df[reject_df["atccode"] == "R01AD"].shape[0] == 2
