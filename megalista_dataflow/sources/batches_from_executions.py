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

from typing import Any, List, Iterable

import apache_beam as beam
import logging

from apache_beam.options.value_provider import ValueProvider
from google.cloud import bigquery
from apache_beam.io.gcp.bigquery import ReadFromBigQueryRequest

from models.execution import DestinationType, Execution, Batch

_BIGQUERY_PAGE_SIZE = 20000

_LOGGER_NAME = 'megalista.BatchesFromExecutions'


def _convert_row_to_dict(row):
    dict = {}
    for key, value in row.items():
        dict[key] = value
    return dict


class BatchesFromExecutions(beam.PTransform):
    """
    Filter the received executions by the received action,
    load the data using the received source and group by that batch size and Execution.
    """

    class _ExecutionIntoBigQueryRequest(beam.DoFn):
        def process(self, execution: Execution) -> Iterable[ReadFromBigQueryRequest]:
            client = bigquery.Client()
            table_name = execution.source.source_metadata[0] + '.' + execution.source.source_metadata[1]
            query = f"SELECT data.* FROM {table_name} AS data"
            logging.getLogger(_LOGGER_NAME).info(f'Reading from table {table_name} for Execution {execution}')
            rows_iterator = client.query(query).result(page_size=_BIGQUERY_PAGE_SIZE)
            for row in rows_iterator:
                yield {'execution': execution, 'row': _convert_row_to_dict(row)}

    class _ExecutionIntoBigQueryRequestTransactional(beam.DoFn):

        def __init__(self, bq_ops_dataset):
            self._bq_ops_dataset = bq_ops_dataset

        def process(self, execution: Execution) -> Iterable[ReadFromBigQueryRequest]:
            table_name = execution.source.source_metadata[0] + \
                '.' + execution.source.source_metadata[1]
            uploaded_table_name = self._bq_ops_dataset.get() + \
                '.' + execution.source.source_metadata[1] + \
                "_uploaded"
            client = bigquery.Client()

            query = f"CREATE TABLE IF NOT EXISTS {uploaded_table_name} ( \
              timestamp TIMESTAMP OPTIONS(description= 'Event timestamp'), \
              uuid STRING OPTIONS(description='Event unique identifier')) \
              PARTITION BY _PARTITIONDATE \
              OPTIONS(partition_expiration_days=15)"

            logging.getLogger(_LOGGER_NAME).info(
                f"Creating table {uploaded_table_name} if it doesn't exist")

            client.query(query).result()

            query = f"SELECT data.* FROM {table_name} AS data \
                LEFT JOIN {uploaded_table_name} AS uploaded USING(uuid) \
                WHERE uploaded.uuid IS NULL;"

            logging.getLogger(_LOGGER_NAME).info(
                f'Reading from table {table_name} for Execution {execution}')
            rows_iterator = client.query(query).result(page_size=_BIGQUERY_PAGE_SIZE)
            for row in rows_iterator:
                yield {'execution': execution, 'row': _convert_row_to_dict(row)}


    class _BatchElements(beam.DoFn):
        def __init__(self, batch_size: int):
            self._batch_size = batch_size

        def process(self, grouped_elements):
            # grouped_elements[0] is the grouping key, the execution
            execution = grouped_elements[0]
            batch: List[Any] = []
            # grouped_elements[1] is the list of elements
            for i, element in enumerate(grouped_elements[1]):
                if i != 0 and i % self._batch_size == 0:
                    yield Batch(execution, batch)
                    batch = []
                batch.append(element['row'])
            yield Batch(execution, batch)

    def __init__(
        self,
        destination_type: DestinationType,
        batch_size: int = 5000,
        transactional: bool = False,
        bq_ops_dataset: ValueProvider = None
    ):
        super().__init__()
        if transactional and not bq_ops_dataset:
            raise Exception('Missing bq_ops_dataset for this uploader')

        self._destination_type = destination_type
        self._batch_size = batch_size
        self._transactional = transactional
        self._bq_ops_dataset = bq_ops_dataset

    def _get_bq_request_class(self):
        if self._transactional:
            return self._ExecutionIntoBigQueryRequestTransactional(self._bq_ops_dataset)
        return self._ExecutionIntoBigQueryRequest()

    def expand(self, executions):
        return (
            executions
            | beam.Filter(lambda execution: execution.destination.destination_type == self._destination_type)
            | beam.ParDo(self._get_bq_request_class())
            | beam.GroupBy(lambda x: x['execution'])
            | beam.ParDo(self._BatchElements(self._batch_size))
        )
