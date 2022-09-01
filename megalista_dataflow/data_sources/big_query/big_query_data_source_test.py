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

from models.execution import AccountConfig, ExecutionsGroupedBySource
from models.execution import Destination
from models.execution import DestinationType
from models.execution import Execution
from models.execution import Source
from models.execution import SourceType
from models.execution import Batch
import pytest
import logging

from models.execution import TransactionalType

from google.cloud.bigquery import SchemaField

from data_sources.big_query.big_query_data_source import BigQueryDataSource

table_name = 'table_01'
bq_ops_dataset = 'bq_ops_dataset'
bq_location = 'us-east1'
data_dataset = 'data_dataset'
source_metadata = [data_dataset, table_name]
table_name_uploaded = f'{table_name}_uploaded'

def _get_executions():
  return ExecutionsGroupedBySource(
    'source_name',
    [Execution(
      None,
      Source(
        'source_name',
        SourceType.BIG_QUERY,
        []
      ),
      Destination(
        'destination_name',
        DestinationType.ADS_CUSTOMER_MATCH_MOBILE_DEVICE_ID_UPLOAD,
        []
      )
    )]
  )

@pytest.fixture
def bq_data_source_uuid():
  return BigQueryDataSource(_get_executions(), TransactionalType.UUID, bq_ops_dataset, bq_location)

@pytest.fixture
def bq_data_source_gclid_time():
  return BigQueryDataSource(_get_executions(), TransactionalType.GCLID_TIME, bq_ops_dataset, bq_location)

@pytest.fixture
def bq_data_source_non_transactional():
  return BigQueryDataSource(_get_executions(), TransactionalType.NOT_TRANSACTIONAL, bq_ops_dataset, bq_location)

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


def test_bigquery_write_transactional_uuid(mocker, bq_data_source_uuid):
  bq_client = mocker.MagicMock()
  now = datetime.datetime.now().timestamp()

  mocker.patch.object(bq_data_source_uuid, "_get_bq_client")
  bq_data_source_uuid._get_bq_client.return_value = bq_client
  mocker.patch.object(bq_data_source_uuid, "_get_now")
  bq_data_source_uuid._get_now.return_value = now

  table = mocker.MagicMock()
  bq_client.get_table.return_value = table

  account_config = AccountConfig("account_id", False, "ga_account_id", "", "")
  destination = Destination(
      "dest1",
      DestinationType.GA_MEASUREMENT_PROTOCOL,
      ["web_property", "view", "c", "list", "d", "buyers_custom_dim"])
  source = Source("orig1", SourceType.BIG_QUERY, ["dt1", "buyers"])
  execution = Execution(account_config, source, destination)

  bq_data_source_uuid.write_transactional_info([{"uuid": "uuid-1"}, {"uuid": "uuid-2"}], execution)

  bq_client.insert_rows.assert_called_once_with(
      table,
      [{"uuid": "uuid-1", "timestamp": now},
       {"uuid": "uuid-2", "timestamp": now}],
      (SchemaField("uuid", "string"),
       SchemaField("timestamp", "timestamp")))


def test_bigquery_write_transactional_gclid_time(mocker, bq_data_source_gclid_time):
  bq_client = mocker.MagicMock()
  now = datetime.datetime.now().timestamp()

  mocker.patch.object(bq_data_source_gclid_time, "_get_bq_client")
  bq_data_source_gclid_time._get_bq_client.return_value = bq_client
  mocker.patch.object(bq_data_source_gclid_time, "_get_now")
  bq_data_source_gclid_time._get_now.return_value = now

  table = mocker.MagicMock()
  bq_client.get_table.return_value = table


  account_config = AccountConfig("account_id", False, "ga_account_id", "", "")
  destination = Destination(
    "dest1",
    DestinationType.ADS_OFFLINE_CONVERSION,
    ["conversion"])
  source = Source("orig1", SourceType.BIG_QUERY, ["dt1", "buyers"])
  execution = Execution(account_config, source, destination)

  bq_data_source_gclid_time.write_transactional_info(
                                  [{"gclid": "gclid1", "time": "ahorita"},
                                   {"gclid": "gclid2", "time": "ahorita_mas_uno"}], execution)

  bq_client.insert_rows.assert_called_once_with(
    table,
    [{"gclid": "gclid1", "time": "ahorita", "timestamp": now},
     {"gclid": "gclid2", "time": "ahorita_mas_uno", "timestamp": now}],
    (SchemaField("gclid", "string"),
     SchemaField("time", "string"),
     SchemaField("timestamp", "timestamp")))

def test_bigquery_write_failure(mocker, bq_data_source_uuid, caplog):
  bq_client = mocker.MagicMock()

  mocker.patch.object(bq_data_source_uuid, "_get_bq_client")
  bq_data_source_uuid._get_bq_client.return_value = bq_client

  error_message = "This is an error message"
  bq_client.insert_rows.return_value = [{"errors": error_message}]

  account_config = AccountConfig("account_id", False, "ga_account_id", "", "")
  source = Source("orig1", SourceType.BIG_QUERY, ["dt1", "buyers"])
  destination = Destination(
      "dest1",
      DestinationType.GA_MEASUREMENT_PROTOCOL,
      ["web_property", "view", "c", "list", "d", "buyers_custom_dim"])

  execution = Execution(account_config, source, destination)

  bq_data_source_uuid.write_transactional_info([{"uuid": "uuid-1"}], execution)

  assert error_message in caplog.text