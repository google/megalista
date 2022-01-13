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
from data_sources.file.file_data_source import FileDataSource
from data_sources.big_query.big_query_data_source import BigQueryDataSource

import importlib

_LOGGER_NAME = 'megalista.data_sources.DataSource'

class DataSource:
    def get_data_source(source_type: SourceType, destination_type: DestinationType, isTransactional: bool, dataflow_options: DataflowOptions, args: dict) -> BaseDataSource:
        data_source = None
        if source_type == SourceType.BIG_QUERY:
            bq_ops_dataset = None
            if 'bq_ops_dataset' in args:
                bq_ops_dataset = args['bq_ops_dataset'].get()
            data_source = BigQueryDataSource(isTransactional, bq_ops_dataset)
        elif source_type == SourceType.FILE:
            data_source = FileDataSource(isTransactional, dataflow_options, destination_type)
        else:
            pass
        if data_source is None:
            logging.getLogger(_LOGGER_NAME).error(f'Source type "{source_type}" not found.')
        return data_source
