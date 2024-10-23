"""Optimized journal test module
"""

from areas.optimized.jobs.journal import Job

job = Job(is_test=True)

def test_optimized_journal():
    """Optimized journal job test function
    """
    correct_df, reject_df = job.start()
    assert correct_df.shape[0] == 10
    assert reject_df.shape[0] == 0
