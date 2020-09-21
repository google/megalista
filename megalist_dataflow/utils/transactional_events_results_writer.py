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

import logging
from datetime import datetime

import apache_beam as beam
from google.cloud import bigquery
from google.cloud.bigquery import SchemaField

from uploaders import google_ads_utils as ads_utils
from uploaders import utils


class TransactionalEventsResultsWriter(beam.DoFn):
  """
  Uploads UUIDs from rows successfully sent to Analytics' Measurement Protocol.
  It uploads the rows to a table with the same name of the source table plus the suffix '_uploaded'.
  """

  def __init__(self, bq_ops_dataset):
    super().__init__()
    self._bq_ops_dataset = str(bq_ops_dataset)

  @utils.safe_process(logger=logging.getLogger("megalista.TransactionalEventsResultsWriter"))
  def process(self, elements, *args, **kwargs):
    self._do_process(elements, datetime.now().timestamp())

  def _do_process(self, elements, now):
    ads_utils.assert_elements_have_same_execution(elements)
    any_execution = elements[0]['execution']
    #ads_utils.assert_right_type_action(any_execution, DestinationType.GA_MEASUREMENT_PROTOCOL)

    table_name = self._bq_ops_dataset + '.' + any_execution.source.source_metadata[1] + "_uploaded"

    rows = utils.extract_rows(elements)
    client = self._get_bq_client()
    table = client.get_table(table_name)
    print(table_name)
    results = client.insert_rows(table, [{'uuid': row['uuid'], 'timestamp': now} for row in rows],
                                 (SchemaField("uuid", "string"), SchemaField("timestamp", "timestamp")))

    for result in results:
      logging.getLogger("megalista.TransactionalEventsResultsWriter").error(result['errors'])

  @staticmethod
  def _get_bq_client():
    return bigquery.Client()
