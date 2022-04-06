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

import datetime

from models.execution import AccountConfig
from models.execution import Destination
from models.execution import DestinationType
from models.execution import Execution
from models.execution import Source
from models.execution import SourceType
from models.execution import Batch
import pytest

from models.execution import TransactionalType

from google.cloud.bigquery import SchemaField

from data_sources.big_query.big_query_data_source import BigQueryDataSource

table_name = 'table_01'
bq_ops_dataset = 'bq_ops_dataset'
data_dataset = 'data_dataset'
source_metadata = [data_dataset, table_name]
table_name_uploaded = f'{table_name}_uploaded'


@pytest.fixture
def bq_data_source_uuid():
  return BigQueryDataSource(TransactionalType.UUID, bq_ops_dataset)


@pytest.fixture
def bq_data_source_gclid_time():
  return BigQueryDataSource(TransactionalType.GCLID_TIME, bq_ops_dataset)

@pytest.fixture
def bq_data_source_non_transactional():
  return BigQueryDataSource(TransactionalType.NOT_TRANSACTIONAL, bq_ops_dataset)

def test_get_table_name(mocker, bq_data_source_uuid, bq_data_source_gclid_time, bq_data_source_non_transactional):
  result_transactional_uploaded = f'{bq_ops_dataset}.{table_name_uploaded}'
  result_transactional_non_uploaded = f'{data_dataset}.{table_name}'
  result_non_transactional = f'{data_dataset}.{table_name}'

  assert bq_data_source_uuid._get_table_name(source_metadata, True) == result_transactional_uploaded
  assert bq_data_source_gclid_time._get_table_name(source_metadata, True) == result_transactional_uploaded
  assert bq_data_source_uuid._get_table_name(source_metadata, False) == result_transactional_non_uploaded
  assert bq_data_source_gclid_time._get_table_name(source_metadata, False) == result_transactional_non_uploaded
  assert bq_data_source_non_transactional._get_table_name(source_metadata, False) == result_non_transactional

def test_get_schema_fields(mocker, bq_data_source_uuid, bq_data_source_gclid_time):
  assert bq_data_source_uuid._get_schema_fields() == (SchemaField("uuid", "string"), SchemaField("timestamp", "timestamp"))
  assert bq_data_source_gclid_time._get_schema_fields() == (SchemaField("gclid", "string"), SchemaField("time", "string"), SchemaField("timestamp", "timestamp"))
