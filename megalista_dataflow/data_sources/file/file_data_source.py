# Copyright 2021 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import pandas as pd
import io
import os
from datetime import datetime, timedelta
from typing import Any, List, Iterable, Tuple, Dict

import apache_beam as beam
import logging

from models.execution import DestinationType, Execution, Batch, TransactionalType
from models.options import DataflowOptions


from data_sources.base_data_source import BaseDataSource
from data_sources.file.file_provider import FileProvider
from data_sources.data_schemas import DataSchemas

_LOGGER_NAME = 'megalista.data_sources.File'


class FileDataSource(BaseDataSource):
    def __init__(self, transactional_type: TransactionalType, dataflow_options: DataflowOptions, destination_type: DestinationType):
        self._transactional_type = transactional_type
        self._dataflow_options = dataflow_options
        self._destination_type = destination_type
  
    def retrieve_data(self, execution: Execution) -> Iterable[Tuple[Execution, Dict[str, Any]]]:
        if self._transactional_type == TransactionalType.NOT_TRANSACTIONAL:
            return self._retrieve_data_non_transactional(execution)
        else:
            return self._retrieve_data_transactional(execution)
  
    def _retrieve_data_non_transactional(self, execution: Execution) -> Iterable[Tuple[Execution, Dict[str, Any]]]:
        # Get Data Source
        data_source = self._get_data_source(execution.source.source_metadata[0])
        # Get Data Frame
        df = data_source.get_data_frame(execution.source.source_metadata[1])
        if df is not None:
            # Process Data Frame
            for index, row in df.iterrows():
                yield execution, FileDataSource._convert_row_to_dict(row)
  
    def _retrieve_data_transactional(self, execution: Execution) -> Iterable[Tuple[Execution, Dict[str, Any]]]:
        # Get Data Source
        data_source = self._get_data_source(execution.source.source_metadata[0])
        # Get Data Frame
        df = data_source.get_data_frame(execution.source.source_metadata[1])
        # Get Uploaded Data Frame
        df_uploaded = data_source.get_data_frame(execution.source.source_metadata[1], is_uploaded=True)
        
        if df is not None:
            # Get items that haven't been processed yet
            df_merged = df.merge(df_uploaded, how='outer')
            df_distinct = df_merged.drop(df_merged[df_merged.timestamp.notnull()].index)
            # Process Data Frame
            for index, row in df_distinct.iterrows():
                yield execution, FileDataSource._convert_row_to_dict(row)

    def write_transactional_info(self, rows, execution: Execution):
        # Get Data Source
        data_source = self._get_data_source(execution.source.source_metadata[0])
        # Get Data Frame
        df = data_source.get_data_frame(execution.source.source_metadata[1], is_uploaded=True)
        
        now = datetime.now()

        if df is None:
            pass
        else:
            # Insert data
            new_df = None
            if self._transactional_type == TransactionalType.UUID:
                new_df = pd.DataFrame([{'uuid': row['uuid'], 'timestamp': now} for row in rows])
            elif self._transactional_type == TransactionalType.GCLID_TIME:
                new_df = pd.DataFrame({'gclid': row['gclid'], 'time': row['time'], 'timestamp': now} for row in rows)
            df = df.append(new_df, ignore_index=True)
            # Upload file
            # Add _uploaded into path
            path = FileDataSource._append_filename_uploaded(execution.source.source_metadata[1])
            bytes = data_source._get_file_from_data_frame(df).getbuffer().tobytes()
            FileProvider(path, self._dataflow_options).write(bytes)

    def get_data_frame(self, path: str, is_uploaded: bool = False) -> pd.DataFrame:
        # Change filename if uploaded
        if is_uploaded:
            # Add _uploaded into path
            path = FileDataSource._append_filename_uploaded(path)
        
        # Retrieve file
        file = io.BytesIO(FileProvider(path, self._dataflow_options).read())

        # Convert file into Data Frame
        df = None
        if file.getbuffer().nbytes == 0:
            if is_uploaded:
                df = pd.DataFrame({'uuid': [], 'timestamp': []})
                pass
            else:
                logging.getLogger(_LOGGER_NAME).error(f'Unable to find data source. path="{path}"')
        else:
            df = self._get_data_frame_from_file(file)
            # if uploaded, drop items older than 15 days
            if is_uploaded:
                now = datetime.now()
                cut_timestamp = now - timedelta(days=15)
                df['timestamp'] = pd.to_datetime(df['timestamp'])
                df.drop(df[df.timestamp < cut_timestamp].index, inplace=True)

        return df
    
    def _append_filename_uploaded(path: str) -> str:
        # Case where things might go wrong: Windows-based systems where path separator is '\' instead of '/'
        base_path = os.path.splitext(path)[0]
        base_path = base_path + '_uploaded'
        return base_path + os.path.splitext(path)[1]

    def _get_data_source(self, file_type: str):
        data_source = None
        file_type = file_type.upper()
        if file_type == 'PARQUET':
            data_source = ParquetDataSource(self._is_transactional, self._dataflow_options, self._destination_type)
        elif file_type == 'CSV':
            data_source = CSVDataSource(self._is_transactional, self._dataflow_options, self._destination_type)
        
        if data_source is None:
            logging.getLogger(_LOGGER_NAME).error(f'Data source not found. Please check your source in config (value={file_type}).')
        
        return data_source

    def _convert_row_to_dict(row):
        dict = {}
        for key, value in row.items():
            dict[key] = value
        return dict
    
    def _update_dtypes(self, destination_type: DestinationType, col_names: list) -> dict:
        types_dict = DataSchemas[destination_type.name]
        types_dict.update({col: "string" for col in col_names if col not in types_dict})
        return types_dict

    def _get_data_frame_from_file(self, file: io.BytesIO) -> pd.DataFrame:
        logging.getLogger(_LOGGER_NAME).error(f'Data source not defined. Please call FileDataSource._get_data_source for defining the correct data source.')

    def _get_file_from_data_frame(self, df: pd.DataFrame) -> io.BytesIO:
        logging.getLogger(_LOGGER_NAME).error(f'Data source not defined. Please call FileDataSource._get_data_source for defining the correct data source.')


class ParquetDataSource(FileDataSource):
    def _get_data_frame_from_file(self, file: io.BytesIO) -> pd.DataFrame:
        df = pd.read_parquet(file)
        DataSchemas.process_by_destination_type(df, self._destination_type)
        return df

    def _get_file_from_data_frame(self, df: pd.DataFrame) -> io.BytesIO:
        to_write = io.BytesIO()
        df.to_parquet(to_write)
        return to_write

class CSVDataSource(FileDataSource):
    def _get_data_frame_from_file(self, file: io.BytesIO) -> pd.DataFrame:
        df = pd.read_csv(file, dtype='string')
        df = DataSchemas.update_data_types_not_string(df, self._destination_type)
        DataSchemas.process_by_destination_type(df, self._destination_type)
        return df

    def _get_file_from_data_frame(self, df: pd.DataFrame) -> io.BytesIO:
        to_write = io.BytesIO()
        df.to_csv(to_write, index=False)
        return to_write
