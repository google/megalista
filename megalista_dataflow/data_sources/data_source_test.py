# Copyright 2022 Google LLC
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

from data_sources.data_source import DataSource
from data_sources.big_query.big_query_data_source import BigQueryDataSource
from data_sources.file.file_data_source import FileDataSource
from models.execution import SourceType, DestinationType, TransactionalType, Execution, Source, Destination, ExecutionsGroupedBySource
from models.options import DataflowOptions

def _get_executions(source_type, source_name, destination_type, destination_name):
    return ExecutionsGroupedBySource(
        source_name,
        [Execution(
        None,
        Source(
            source_name,
            source_type,
            []
        ),
        Destination(
            destination_name,
            destination_type,
            []
        )
        )]
    )

def test_get_big_query_data_source(mocker):
    source_type = SourceType.BIG_QUERY
    source_name = 'source_name'
    destination_type = DestinationType.ADS_CUSTOMER_MATCH_CONTACT_INFO_UPLOAD
    destination_name = 'destination_name'
    transactional_type = TransactionalType.NOT_TRANSACTIONAL
    dataflow_options = mocker.MagicMock()
    
    data_source = DataSource.get_data_source(_get_executions(source_type, source_name, destination_type, destination_name), transactional_type, dataflow_options)

    assert type(data_source) is BigQueryDataSource
    
def test_get_file_data_source(mocker):
    source_type = SourceType.FILE
    source_name = 'source_name'
    destination_type = DestinationType.ADS_CUSTOMER_MATCH_CONTACT_INFO_UPLOAD
    destination_name = 'destination_name'
    transactional_type = TransactionalType.NOT_TRANSACTIONAL
    dataflow_options = mocker.MagicMock()
    
    data_source = DataSource.get_data_source(_get_executions(source_type, source_name, destination_type, destination_name), transactional_type, dataflow_options)

    assert type(data_source) is FileDataSource
    
