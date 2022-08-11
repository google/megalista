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

"""
This module is responsible for handling (read, write) data from files. As of now, it handles CSV and Parquet files as data sources.
It depends on file_provider module, which is responsible for handling file operations.
"""

import pandas as pd
import io
import os
from datetime import datetime, timedelta
from typing import Any, List, Iterable, Tuple, Dict
from apache_beam.typehints.decorators import with_output_types
import numpy as np

import logging

from models.execution import SourceType, DestinationType, Execution, Batch, TransactionalType, ExecutionsGroupedBySource, DataRowsGroupedBySource
from models.options import DataflowOptions


from data_sources.base_data_source import BaseDataSource
from data_sources.file.file_provider import FileProvider
import data_sources.data_schemas as DataSchemas

_LOGGER_NAME = 'megalista.data_sources.File'


class FileDataSource(BaseDataSource):
    def __init__(self, 
        executions: ExecutionsGroupedBySource, transactional_type: TransactionalType, 
        dataflow_options: DataflowOptions):
        super().__init__(executions, transactional_type)
        self._dataflow_options = dataflow_options
  
    def retrieve_data(self, executions: ExecutionsGroupedBySource) -> List[Any]:
        if self._transactional_type == TransactionalType.NOT_TRANSACTIONAL:
            return self._retrieve_data_non_transactional(executions)
        else:
            data = self._retrieve_data_transactional(executions)
            return data
  
    def _retrieve_data_non_transactional(self, executions: ExecutionsGroupedBySource) -> List[DataRowsGroupedBySource]:
        source = executions.source
        # Get Data Source
        data_source = self._get_data_source(source.source_name, source.source_metadata[0])
        # Get Data Frame
        df = data_source.get_data_frame(source.source_name, source.source_metadata[1])
        logging.getLogger(_LOGGER_NAME).info(f'Data source ({self._source_name}): using {len(df.index)} rows')
        if df is not None:
            df = df.fillna(np.nan).replace([np.nan], [None])
            # Process Data Frame
            elements = []
            for index, row in df.iterrows():
                elements.append(FileDataSource._convert_row_to_dict(row))
            return [DataRowsGroupedBySource(executions, elements)]
        else:
            raise Exception(f'Unable to read from data source. Source="{source.source_name}".')
    
    @with_output_types(DataRowsGroupedBySource)
    def _retrieve_data_transactional(self, executions: ExecutionsGroupedBySource) -> List[DataRowsGroupedBySource]:
        source = executions.source
        # Get Data Source
        data_source = self._get_data_source(source.source_name, source.source_metadata[0])
        # Get Data Frame
        df = data_source.get_data_frame(source.source_name, source.source_metadata[1])
        # Get Uploaded Data Frame
        df_uploaded = data_source.get_data_frame(source.source_name, source.source_metadata[1], is_uploaded=True)
        
        if df is not None:
            # Get items that haven't been processed yet
            df_merged = df.merge(df_uploaded, how='outer')
            df_distinct = df_merged.drop(df_merged[df_merged.timestamp.notnull()].index)
            # Process Data Frame
            df_distinct = df_distinct.fillna(np.nan).replace([np.nan], [None])
            logging.getLogger(_LOGGER_NAME).info(f'Data source ({self._source_name}): using {len(df_distinct.index)} rows')
            elements = []
            for index, row in df_distinct.iterrows():
                elements.append(FileDataSource._convert_row_to_dict(row))
            return [DataRowsGroupedBySource(executions, elements)]
        else:
            raise Exception(f'Unable to read from data source. Source="{self._source_name}". Destination="{self._destination_name}"')

    def write_transactional_info(self, rows, execution: Execution):
        # Get Data Source
        data_source = self._get_data_source(execution.source.source_name, execution.source.source_metadata[0])
        # Get Data Frame
        df = data_source.get_data_frame(execution.source.source_name, execution.source.source_metadata[1], is_uploaded=True)
        
        now = datetime.now()

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
        FileProvider(path, self._dataflow_options, self._source_type, self._source_name, False).write(bytes)

    def get_data_frame(self, source_name: str, path: str, is_uploaded: bool = False) -> pd.DataFrame:
        # Change filename if uploaded
        if is_uploaded:
            # Add _uploaded into path
            path = FileDataSource._append_filename_uploaded(path)
        
        # Retrieve file
        file = io.BytesIO(FileProvider(path, self._dataflow_options, self._source_type, self._source_name, is_uploaded).read())

        # Convert file into Data Frame
        if file.getbuffer().nbytes == 0:
            if is_uploaded:
                if self._transactional_type == TransactionalType.UUID:
                    return pd.DataFrame({'uuid': [], 'timestamp': []})
                elif self._transactional_type == TransactionalType.GCLID_TIME:
                    return pd.DataFrame({'gclid': [], 'time': [], 'timestamp': []})
                else:
                    raise NotImplementedError(f'Transactional type not defined: {self._transactional_type.name}. Source="{self._source_name}". Destination="{self._destination_name}"')
            else:
                raise ValueError(f'Unable to find file: "{path}". Source="{self._source_name}". Destination="{self._destination_name}"')
        else:
            df = self._get_data_frame_from_file(file, is_uploaded)
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

    def _get_data_source(self, source_name: str, file_type: str):
        file_type = file_type.upper()
        if file_type == 'PARQUET':
            return ParquetDataSource(self._executions, self._transactional_type, self._dataflow_options)
        elif file_type == 'CSV':
            return CSVDataSource(self._executions, self._transactional_type, self._dataflow_options)
        raise ValueError(f'Data source not found. Please check your source in config (value={file_type}). Source="{self._source_name}". Destination="{self._destination_name}"')
        
    def _convert_row_to_dict(row):
        dict = {}
        for key, value in row.items():
            dict[key] = value
        return dict
    
    def _update_dtypes(self, destination_type: DestinationType, col_names: list) -> dict:
        types_dict = DataSchemas[destination_type.name]
        types_dict.update({col: "string" for col in col_names if col not in types_dict})
        return types_dict

    def _get_data_frame_from_file(self, file: io.BytesIO, is_uploaded: bool) -> pd.DataFrame:
        raise NotImplementedError(f'Data source not defined. Please call FileDataSource._get_data_source for defining the correct data source. Source="{self._source_name}". Destination="{self._destination_name}"')

    def _get_file_from_data_frame(self, df: pd.DataFrame) -> io.BytesIO:
        raise NotImplementedError(f'Data source not defined. Please call FileDataSource._get_data_source for defining the correct data source. Source="{self._source_name}". Destination="{self._destination_name}"')


class ParquetDataSource(FileDataSource):
    def _get_data_frame_from_file(self, file: io.BytesIO, is_uploaded: bool) -> pd.DataFrame:
        destination_type = self._destination_type
        if is_uploaded:
            destination_type = DestinationType.UPLOADED_GCLID_TIME if self._transactional_type == TransactionalType.GCLID_TIME else DestinationType.UPLOADED_UUID
        cols = pd.read_parquet(file).columns
        if DataSchemas.validate_data_columns(cols, destination_type):
            cols = DataSchemas.get_cols_names(cols, destination_type)
            file.seek(0)
            df = pd.read_parquet(file, dtype='string', columns=cols)
            df = DataSchemas.update_data_types_not_string(df, destination_type)
            df = DataSchemas.process_by_destination_type(df, destination_type)
            return df
        else:
            raise ValueError(f'Data source incomplete. {DataSchemas.get_error_message(cols, self._destination_type)} Source="{self._source_name}". Destination="{self._destination_name}"')

    def _get_file_from_data_frame(self, df: pd.DataFrame) -> io.BytesIO:
        to_write = io.BytesIO()
        df.to_parquet(to_write)
        return to_write

class CSVDataSource(FileDataSource):
    def _get_data_frame_from_file(self, file: io.BytesIO, is_uploaded: bool) -> pd.DataFrame:
        destination_type = self._destination_type
        if is_uploaded:
            destination_type = DestinationType.UPLOADED_GCLID_TIME if self._transactional_type == TransactionalType.GCLID_TIME else DestinationType.UPLOADED_UUID
        cols = pd.read_csv(file, dtype='string', nrows=0).columns
        if DataSchemas.validate_data_columns(cols, destination_type):
            cols = DataSchemas.get_cols_names(cols, destination_type)
            file.seek(0)
            df = pd.read_csv(file, dtype='string', usecols=cols)
            df = DataSchemas.update_data_types_not_string(df, destination_type)
            DataSchemas.process_by_destination_type(df, destination_type)
            return df
        else:
            raise ValueError(f'Data source incomplete. {DataSchemas.get_error_message(cols, self._destination_type)} Source="{self._source_name}". Destination="{self._destination_name}"')
        
    def _get_file_from_data_frame(self, df: pd.DataFrame) -> io.BytesIO:
        return io.BytesIO(df.to_csv(index=False).encode())
