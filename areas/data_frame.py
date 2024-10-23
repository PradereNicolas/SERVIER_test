"""Dataframe utils module
"""

from dataclasses import dataclass
from typing import Optional

@dataclass
class Column:
    """Dataframe column properties
    """
    name: str
    type_: type
    required: bool
    is_functional_key: Optional[bool] = False
    