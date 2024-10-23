"""Test utils module
"""

import pandas as pd

def check_reject_comment(data_frame_record: pd.Series, reject_reason: str):
    """_summary_

    Args:
        data_frame_record (pd.Series): Daraframe record
        reject_reason (str): Expected reject reason

    Raises:
        ValueError: Error raised if criteria returns multiple records
        ValueError: Error raised if criteria returns no record
    """
    value = data_frame_record["reject_reason"]
    if not isinstance(value, pd.Series):
        assert value == reject_reason, f"Reject reason id not correct: [{value}] not equal to [{reject_reason}]"
        return
    value = data_frame_record["reject_reason"].values
    if len(value) > 1:
        raise ValueError("Multiple records returned by these conditions, please modify criteria")
    if len(value) == 0:
        raise ValueError("No record returned by these conditions, please modify criteria")
    value = value[0]
    assert value == reject_reason, f"Reject reason id not correct: [{value}] not equal to [{reject_reason}]"
