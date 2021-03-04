# Copyright 2020 Google LLC
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
from uploaders.big_query.transactional_events_results_writer import TransactionalEventsResultsWriter

from google.cloud.bigquery import SchemaField

from apache_beam.options.value_provider import StaticValueProvider


@pytest.fixture
def uploader():
  return TransactionalEventsResultsWriter(StaticValueProvider(str, 'bq_ops_dataset'))


def test_bigquery_write(mocker, uploader):
  bq_client = mocker.MagicMock()

  mocker.patch.object(uploader, "_get_bq_client")
  uploader._get_bq_client.return_value = bq_client

  table = mocker.MagicMock()
  bq_client.get_table.return_value = table

  now = datetime.datetime.now().timestamp()

  account_config = AccountConfig("account_id", False, "ga_account_id", "", "")
  destination = Destination(
      "dest1",
      DestinationType.GA_MEASUREMENT_PROTOCOL,
      ["web_property", "view", "c", "list", "d", "buyers_custom_dim"])
  source = Source("orig1", SourceType.BIG_QUERY, ["dt1", "buyers"])
  execution = Execution(account_config, source, destination)

  uploader._do_process(Batch(execution, [{"uuid": "uuid-1"}, {"uuid": "uuid-2"}]), now)

  bq_client.insert_rows.assert_called_once_with(
      table,
      [{"uuid": "uuid-1", "timestamp": now},
       {"uuid": "uuid-2", "timestamp": now}],
      (SchemaField("uuid", "string"),
       SchemaField("timestamp", "timestamp")))


def test_bigquery_write_failure(mocker, uploader, caplog):
  bq_client = mocker.MagicMock()

  mocker.patch.object(uploader, "_get_bq_client")
  uploader._get_bq_client.return_value = bq_client

  error_message = "This is an error message"
  bq_client.insert_rows.return_value = [{"errors": error_message}]

  account_config = AccountConfig("account_id", False, "ga_account_id", "", "")
  source = Source("orig1", SourceType.BIG_QUERY, ["dt1", "buyers"])
  destination = Destination(
      "dest1",
      DestinationType.GA_MEASUREMENT_PROTOCOL,
      ["web_property", "view", "c", "list", "d", "buyers_custom_dim"])

  execution = Execution(account_config, source, destination)

  uploader.process(Batch(execution, [{"uuid": "uuid-1"}]))

  assert error_message in caplog.text
