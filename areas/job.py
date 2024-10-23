"""Job module init
"""

from pathlib import Path
from abc import abstractmethod, ABC
import re
from typing import Tuple, Optional, Any

import numpy as np
import pandas as pd

from areas.data_frame import Column

from areas.utils import generate_technical_id

from .area import DataFormat, AreaType

from .raw import DataType as RawDataType

class DefaultJob(ABC):
    """Default job class
    """
    class Source:
        """Data source for job
        """
        def __init__(
            self,
            area: AreaType,
            data_type: RawDataType,
            data_format: DataFormat,
            is_test: bool = False) -> None:
            """Source data class

            Args:
                area (AreaType): Soure area
                data_type (RawDataType): Source data type
                data_format (DataFormat): Source data format
                is_test (bool): Boolean value indicating if the source is for test.Defaults to False
            """
            self.area = area
            self.data_type = data_type
            self.data_format = data_format
            self.is_test = is_test

        @property
        def path(self) -> str:
            """Get source file path

            Returns:
                str: Source file path
            """
            base_path = "tests" if self.is_test else "areas"
            folder_path =  f"{base_path}/{self.area.value}/data" \
                if self.is_test else f"{base_path}/{self.area.value}/data/{self.data_type.value}"
            if self.is_test is False and self.data_format != DataFormat.PICKLE:
                folder_path = f"{folder_path}/{self.data_format.value}"
            return f"{folder_path}/{self.data_type.value}.{self.data_format.value}"

        def set_to_test(self) -> None:
            """Change class properties to be test source
            """
            self.is_test = True
            if self.data_format == DataFormat.PICKLE:
                self.data_format = DataFormat.CSV

    def __init__(
        self,
        target_area: AreaType,
        target_data_type: Any,
        target_schema: list[Column],
        sources: list[Source],
        is_test: bool = False) -> None:
        """Initialize default job method

        Args:
            source_area (AreaType): Area type to get data
            target_area (AreaType): Area type to feed data
            target_schema (list[Column]): Schema of the output dataframe
            sources (Source): Sources data
            is_test (bool): Boolean value indicating if the job is for test.Defaults to False
        """
        self.target_area = target_area
        self.target_data_type = target_data_type
        self.sources = sources
        self.target_schema = target_schema
        self.is_test = is_test
        if self.is_test:
            for source in self.sources:
                source.set_to_test()

    @property
    def target_folder_path(self) -> str:
        """Target folder

        Returns:
            str: Targer folder
        """
        return f"areas/{self.target_area.value}/data/{self.target_data_type.value}"

    def extract_csv(self, path: Path) -> pd.DataFrame:
        """CSV extract method

        Args:
            path (Path): Path of the file to be extracted

        Returns:
            pd.DataFrame: pandas DataFrame containing data
        """
        return pd.read_csv(path)

    def extract_json(self, path: Path) -> pd.DataFrame:
        """JSON extract method

        Args:
            path (Path): Path of the file to be extracted

        Returns:
            pd.DataFrame: pandas DataFrame containing data
        """
        return pd.read_json(path)

    def extract_pickle(self, path: Path) -> pd.DataFrame:
        """Pickle extract method

        Args:
            path (Path): Path of the file to be extracted

        Returns:
            pd.DataFrame: pandas DataFrame containing data
        """
        return pd.read_pickle(path)

    def extract(self, path: Path) -> pd.DataFrame:
        """Generic method to extract data from source file

        Args:
            path (Path): Path of the file to be extracted

        Returns:
            pd.DataFrame: pandas DataFrame containing data
        """
        match path:
            case path if str(path).endswith(DataFormat.CSV.value):
                return self.extract_csv(path=path)
            case path if str(path).endswith(DataFormat.JSON.value):
                return self.extract_json(path=path)
            case path if str(path).endswith(DataFormat.PICKLE.value):
                return self.extract_pickle(path=path)

    @abstractmethod
    def transform(self, *args, **kwargs) -> None:
        """Default method to be implemented in child classes
        """
        return

    def _process_string_values(self, value: str) -> str:
        """Process string values from records

        Args:
            value (str): String to be processed

        Returns:
            str: Processed string
        """
        # Remove encoded characters
        value = re.sub(r"\\x[0-9a-fA-F]{2}", "", value)
        # Handle encoding
        value = value.encode("utf-8") \
            .decode("utf-8", errors="replace") \
            .strip()
        return None if value == "" else value

    def _check_record_schema(
        self,
        record_values: list,
        schema: list[Column]) -> Tuple[bool, list, Optional[str]]:
        """Check if record respect schema

        Args:
            record_values (list): Record values
            columns (list[Column]): List of columns

        Returns:
            Tuple[bool, list, Optional[str]]: Returns wether the record is correct, \
the values of the record and potentially the reject reason
        """
        current_record = []
        for column, value in zip(schema, record_values):
            # Handle string values for refined layer
            if isinstance(value, str) and self.target_area == AreaType.REFINED:
                value = self._process_string_values(value=value)
            if column.required and pd.isnull(value):
                return False, record_values, f"Column {column.name} should not be empty"
            try:
                # Specific handling for datetime
                if column.type_ == np.datetime64:
                    current_record.append(pd.to_datetime(value, dayfirst=True))
                    continue
                current_record.append(column.type_(value))
            except (ValueError, TypeError):
                return False, record_values, \
                    f"Column {column.name} cannot be converted to {column.type_}"
        return True, current_record, None

    def _apply_schema(
        self,
        data_frame: pd.DataFrame,
        reject_data_frame: pd.DataFrame,
        schema: list[Column]) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Apply dataframe schema

        Args:
            data_frame (pd.DataFrame): Dataframe to be formated
            reject_data_frame (pd.DataFrame): Rejected dataframe
            schema (list[Column]): Dataclass of the schema to be implemented

        Returns:
            Tuple[pd.DataFrame, pd.DataFrame]: Correct dataframe and reject dataframe
        """
        schema = sorted(schema, key=lambda column: column.name)
        data_frame = data_frame[[column.name for column in schema]]
        # Check conversion
        correct_records, rejected_records = [], []
        for _, record in data_frame.iterrows():
            is_record_correct, values, reject_reason = self._check_record_schema(
                record_values=record.values,
                schema=schema
            )
            if is_record_correct:
                correct_records.append(values)
                continue
            rejected_records.append([reject_reason, *values])

        correct_df = pd.DataFrame(
            correct_records,
            columns=[column.name for column in schema])
        # Specific handling for datetime columns
        correct_df.astype({
            column.name: 'datetime64[ns]' if column.type_ == np.datetime64 else column.type_
        for column in schema})

        reject_df = pd.DataFrame(
            rejected_records,
            columns=["reject_reason"] + [column.name for column in schema])

        if reject_data_frame is not None:
            if "reject_reason" not in reject_data_frame.columns:
                raise ValueError("Please provide a reject reason on transform method")
            reject_data_frame = reject_data_frame[reject_df.columns]
            reject_df = pd.concat([
                reject_df.dropna(axis=1, how="all"),
                reject_data_frame.dropna(axis=1, how="all")])

        # Check duplicates on functional keys columns
        for functional_key_column in [column for column in schema if column.is_functional_key]:
            functionnal_key_index_reject_df = correct_df[correct_df.duplicated(
                subset=functional_key_column.name,
                keep=False)]
            if functionnal_key_index_reject_df.shape[0] == 0:
                continue
            correct_df = correct_df.drop_duplicates(
                subset=functional_key_column.name,
                keep=False)
            functionnal_key_index_reject_df.insert(
                loc=0,
                column="reject_reason",
                value=[f"Duplicate value on column {functional_key_column.name}" for _ in \
                    range(functionnal_key_index_reject_df.shape[0])])
            reject_df = pd.concat([reject_df, functionnal_key_index_reject_df])
        return correct_df, reject_df

    def _write_dataframe(self, correct_df: pd.DataFrame, reject_df: pd.DataFrame) -> None:
        """Write dataframe

        Args:
            correct_df (pd.DataFrame): Dataframe to be written
            reject_df (pd.DataFrame): Reject dataframe to be written
        """
        correct_df.to_pickle(
            path=f"{self.target_folder_path}/{self.target_data_type.value}\
.{DataFormat.PICKLE.value}")
        reject_df.to_pickle(
            path=f"{self.target_folder_path}/{self.target_data_type.value}\
_rejected.{DataFormat.PICKLE.value}")

        correct_df.to_csv(
            path_or_buf=f"{self.target_folder_path}/{self.target_data_type.value}\
.{DataFormat.CSV.value}",
            index=False)
        reject_df.to_csv(
            path_or_buf=f"{self.target_folder_path}/{self.target_data_type.value}\
_rejected.{DataFormat.CSV.value}",
            index=False)

    def start(self) -> Optional[Tuple[pd.DataFrame, pd.DataFrame]]:
        """STart job method

        Returns:
            Optional[Tuple[pd.DataFrame, pd.DataFrame]]: Correct dataframe and reject \
dataframe in case of test, else None
        """
        sources = [self.extract(path=source.path) for source in self.sources]

        correct_df, reject_df = self.transform(*sources)

        correct_df, reject_df = self._apply_schema(
            data_frame=correct_df,
            reject_data_frame=reject_df,
            schema=self.target_schema)

        correct_df = generate_technical_id(
            area=self.target_area,
            data_type=self.target_data_type,
            data_frame=correct_df)

        if self.is_test:
            return correct_df, reject_df

        self._write_dataframe(correct_df=correct_df, reject_df=reject_df)
