"""Optimized publication test module
"""

from areas.optimized.jobs.publication import Job
from tests.utils import check_reject_comment

job = Job(is_test=True)

def test_optimized_publication():
    """Optimized publication job test function
    """
    correct_df, reject_df = job.start()
    assert correct_df.shape[0] == 16
    assert reject_df.shape[0] == 1
    check_reject_comment(
        data_frame_record=reject_df[reject_df["functional_id"] == 1],
        reject_reason="Journal not found"
    )
