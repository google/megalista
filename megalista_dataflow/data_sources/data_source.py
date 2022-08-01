# Copyright 2021 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from models.execution import SourceType, DestinationType
from models.options import DataflowOptions
from data_sources.base_data_source import BaseDataSource
from data_sources.big_query.big_query_data_source import BigQueryDataSource
from data_sources.file.file_data_source import FileDataSource
from models.execution import TransactionalType, ExecutionsGroupedBySource

import importlib

_LOGGER_NAME = 'megalista.data_sources.DataSource'

class DataSource:
    def get_data_source(executions: ExecutionsGroupedBySource, transactional_type: TransactionalType, dataflow_options: DataflowOptions) -> BaseDataSource:
        data_source = None
        source_type = executions.source.source_type
        if source_type == SourceType.BIG_QUERY:
            bq_ops_dataset = None
            bq_location = None
            if dataflow_options.bq_ops_dataset:
                bq_ops_dataset = dataflow_options.bq_ops_dataset.get()
            if dataflow_options.bq_location:
                bq_location = dataflow_options.bq_location.get()
            return BigQueryDataSource(executions, transactional_type, bq_ops_dataset, bq_location)
        elif source_type == SourceType.FILE:
            return FileDataSource(executions, transactional_type, dataflow_options)
        else:
            raise NotImplementedError("Source Type not implemented. Please check your configuration (sheet / json / firestore).")
        return data_source
