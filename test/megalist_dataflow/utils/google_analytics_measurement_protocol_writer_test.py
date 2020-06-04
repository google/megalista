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

from datetime import datetime

import pytest
from google.cloud.bigquery import SchemaField

from utils.execution import Execution, AccountConfig, Source, SourceType, Destination, DestinationType
from utils.google_analytics_measurement_protocol_writer import GoogleAnalyticsMeasurementProtocolResultsWriter


@pytest.fixture
def uploader():
    return GoogleAnalyticsMeasurementProtocolResultsWriter()


def test_bigquery_write(mocker, uploader: GoogleAnalyticsMeasurementProtocolResultsWriter):
    bq_client = mocker.MagicMock()

    mocker.patch.object(uploader, "_get_bq_client")
    uploader._get_bq_client.return_value = bq_client

    table = mocker.MagicMock()
    bq_client.get_table.return_value = table

    now = datetime.now().timestamp()

    execution = Execution(AccountConfig("account_id", False, "ga_account_id", "", ""),
                          Source("orig1", SourceType.BIG_QUERY, ["dt1", "buyers"]),
                          Destination("dest1", DestinationType.GA_MEASUREMENT_PROTOCOL,
                                      ["web_property", "view", "c", "list", "d", "buyers_custom_dim"]))

    uploader._do_process(({"execution": execution, "row": {"uuid": "uuid-1"}},
                          {"execution": execution, "row": {"uuid": "uuid-2"}}), now)

    bq_client.insert_rows.assert_called_once_with(table, [{"uuid": "uuid-1", "timestamp": now},
                                                          {"uuid": "uuid-2", "timestamp": now}], (
                                                      SchemaField("uuid", "string"),
                                                      SchemaField("timestamp", "timestamp")))


def test_bigquery_write_failure(mocker, uploader: GoogleAnalyticsMeasurementProtocolResultsWriter, caplog):
    bq_client = mocker.MagicMock()

    mocker.patch.object(uploader, "_get_bq_client")
    uploader._get_bq_client.return_value = bq_client

    error_message = "This is an error message"
    bq_client.insert_rows.return_value = [{"errors": error_message}]

    execution = Execution(AccountConfig("account_id", False, "ga_account_id", "", ""),
                          Source("orig1", SourceType.BIG_QUERY, ["dt1", "buyers"]),
                          Destination("dest1", DestinationType.GA_MEASUREMENT_PROTOCOL,
                                      ["web_property", "view", "c", "list", "d", "buyers_custom_dim"]))

    uploader.process(({"execution": execution, "row": {"uuid": "uuid-1"}},))

    assert error_message in caplog.text
