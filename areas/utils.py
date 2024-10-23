"""Data processing utils module
"""
from typing import Any
import pandas as pd

from areas import AreaType

def generate_functional_key(data_frame: pd.DataFrame, columns: list[str]) -> pd.Series:
    """Generate a functional key from tables

    Args:
        data_frame (pd.DataFrame): Dataframe
        columns (list[str]): List of columns to use for functional key

    Returns:
        pd.Series: New column
    """
    functional_keys = data_frame.apply(lambda x: " ".join(str(x[column]) for column in columns), axis=1)
    functional_keys = functional_keys.transform(lambda x: x.lower().replace(" ", "_"))
    return functional_keys

def generate_technical_id(area: AreaType, data_type: Any, data_frame: pd.DataFrame) -> pd.DataFrame:
    """Create id for data frames

    Args:
        data_frame (pd.DataFrame): Dataframe
        area (AreaType): Area of table
        data_type (Any): Instance

    Returns:
        pd.DataFrame: Dataframe with id
    """
    data_frame.drop(columns="id", errors="ignore", inplace=True)
    data_frame.insert(0, "id", range(data_frame.shape[0]))
    data_frame["id"] = f"{area.name}.{data_type.name}_" + data_frame["id"].astype(str)
    return data_frame
