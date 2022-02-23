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

import logging
from datetime import datetime

import apache_beam as beam
from apache_beam.options.value_provider import ValueProvider
from google.cloud import bigquery
from google.cloud.bigquery import SchemaField

from sources.batches_from_executions import TransactionalType
from uploaders import utils
from models.execution import Batch


class TransactionalEventsResultsWriter(beam.DoFn):
  """
  Uploads UUIDs from rows successfully sent by the uploader.
  It uploads the rows to a table with the same name of the source table plus the suffix '_uploaded'.
  """

  def __init__(self, bq_ops_dataset: ValueProvider, transactional_type : TransactionalType):
    super().__init__()
    self._bq_ops_dataset = bq_ops_dataset
    self._transactional_type = transactional_type

  @utils.safe_process(logger=logging.getLogger("megalista.TransactionalEventsResultsWriter"))
  def process(self, batch: Batch, *args, **kwargs):
    self._do_process(batch, datetime.now().timestamp())

  def _do_process(self, batch: Batch, now):
    execution = batch.execution

    table_name = self._bq_ops_dataset.get() + '.' + execution.source.source_metadata[1] + "_uploaded"

    rows = batch.elements
    client = self._get_bq_client()
    table = client.get_table(table_name)
    results = client.insert_rows(table,
                                 self._get_bq_rows(rows, now),
                                 self._get_schema_fields())

    for result in results:
      logging.getLogger("megalista.TransactionalEventsResultsWriter").error(result['errors'])

  def _get_schema_fields(self):
    if self._transactional_type == TransactionalType.UUID:
      return SchemaField("uuid", "string"), SchemaField("timestamp", "timestamp")
    if self._transactional_type == TransactionalType.GCLID_TIME:
      return SchemaField("gclid", "string"), SchemaField("time", "string"), SchemaField("timestamp", "timestamp")
    raise Exception(f'Unrecognized TransactionalType: {self._transactional_type}')

  def _get_bq_rows(self, rows, now):
    if self._transactional_type == TransactionalType.UUID:
      return [{'uuid': row['uuid'], 'timestamp': now} for row in rows]
    if self._transactional_type == TransactionalType.GCLID_TIME:
      return [{'gclid': row['gclid'], 'time': row['time'], 'timestamp': now} for row in rows]
    raise Exception(f'Unrecognized TransactionalType: {self._transactional_type}')

  @staticmethod
  def _get_bq_client():
    return bigquery.Client()
