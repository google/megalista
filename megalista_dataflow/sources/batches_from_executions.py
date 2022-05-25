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
from enum import Enum

import apache_beam as beam
import logging
import json

from apache_beam.coders import coders
from apache_beam.options.value_provider import ValueProvider
from google.cloud import bigquery
from models.execution import DestinationType, Execution, Batch
from string import Template
from typing import Any, List, Iterable, Optional, Tuple, Dict


_BIGQUERY_PAGE_SIZE = 20000

_LOGGER_NAME = 'megalista.BatchesFromExecutions'


def _convert_row_to_dict(row):
    dict = {}
    for key, value in row.items():
        dict[key] = value
    return dict

def _get_bq_location_or_none(bq_location: ValueProvider = None) -> Optional[str]:
    """Passes bq_location along if there's one. Otherwise, returns None"""
    if not bq_location:
        return bq_location
    return bq_location.get()


class ExecutionCoder(coders.Coder):
    """A custom coder for the Execution class."""

    def encode(self, o):
        return json.dumps(o.to_dict()).encode('utf-8')

    def decode(self, s):
        return Execution.from_dict(json.loads(s.decode('utf-8')))

    def is_deterministic(self):
        return True


class TransactionalType(Enum):
    """
        Distinct types to handle data uploading deduplication.
        NOT_TRANSACTION: don't handle.
        UUID: Expect a 'uuid' field in the source table as a unique identifier to each row.
        GCLID_DATE_TIME: Expect 'gclid' and 'time' fields in the source table as unique identifiers to each row.
    """
    (
        NOT_TRANSACTIONAL,
        UUID,
        GCLID_TIME,
    ) = range(3)


class BatchesFromExecutions(beam.PTransform):
    """
    Filter the received executions by the received action,
    load the data using the received source and group by that batch size and Execution.
    """

    class _ExecutionIntoBigQueryRequest(beam.DoFn):
        def __init__(self, bq_location):
            self._bq_location = bq_location

        def process(self, execution: Execution) -> Iterable[Tuple[Execution, Dict[str, Any]]]:
            client = bigquery.Client()
            bq_location = _get_bq_location_or_none(self._bq_location)
            table_name = execution.source.source_metadata[0] + '.' + execution.source.source_metadata[1]
            table_name = table_name.replace('`', '')
            query = f"SELECT data.* FROM `{table_name}` AS data"
            logging.getLogger(_LOGGER_NAME).info(f'Reading from table {table_name} for Execution {execution}')
            for row in client.query(query, location=bq_location).result(page_size=_BIGQUERY_PAGE_SIZE):
                yield execution, _convert_row_to_dict(row)

    class _ExecutionIntoBigQueryRequestTransactional(beam.DoFn):

        def __init__(self, bq_ops_dataset, bq_location, create_table_query, join_query):
            self._bq_ops_dataset = bq_ops_dataset
            self._create_table_query = create_table_query
            self._join_query = join_query
            self._bq_location = bq_location

        def process(self, execution: Execution) -> Iterable[Tuple[Execution, Dict[str, Any]]]:
            table_name = execution.source.source_metadata[0] + \
                '.' + execution.source.source_metadata[1]
            table_name = table_name.replace('`', '')
            uploaded_table_name = self._bq_ops_dataset.get() + \
                '.' + execution.source.source_metadata[1] + \
                "_uploaded"
            uploaded_table_name = uploaded_table_name.replace('`', '')
            client = bigquery.Client()
            bq_location = _get_bq_location_or_none(self._bq_location)

            create_table_query_ready = \
                Template(self._create_table_query).substitute(uploaded_table_name=uploaded_table_name)

            logging.getLogger(_LOGGER_NAME).info(
                f"Creating table {uploaded_table_name} if it doesn't exist")

            client.query(create_table_query_ready, location=bq_location).result()

            join_query_ready = \
                Template(self._join_query).substitute(table_name=table_name, uploaded_table_name=uploaded_table_name)

            logging.getLogger(_LOGGER_NAME).info(
                f'Reading from table {table_name} for Execution {execution}')
            for row in client.query(join_query_ready, location=bq_location).result(page_size=_BIGQUERY_PAGE_SIZE):
                yield execution, _convert_row_to_dict(row)


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
                batch.append(element)
            yield Batch(execution, batch)

    def __init__(
        self,
        destination_type: DestinationType,
        bq_location: ValueProvider,
        batch_size: int = 5000,
        transactional_type: TransactionalType = TransactionalType.NOT_TRANSACTIONAL,
        bq_ops_dataset: ValueProvider = None    
    ):
        super().__init__()
        if transactional_type is not TransactionalType.NOT_TRANSACTIONAL:
            if not bq_ops_dataset or not bq_location:
                raise Exception('Missing bq_ops_dataset/bq_location for this uploader')

        self._destination_type = destination_type
        self._batch_size = batch_size
        self._transactional_type = transactional_type
        self._bq_ops_dataset = bq_ops_dataset
        self._bq_location = bq_location

    def _get_bq_request_class(self):
        if self._transactional_type == TransactionalType.UUID:
            return self._ExecutionIntoBigQueryRequestTransactional(
                self._bq_ops_dataset,
                self._bq_location,
                "CREATE TABLE IF NOT EXISTS `$uploaded_table_name` ( \
                             timestamp TIMESTAMP OPTIONS(description= 'Event timestamp'), \
                             uuid STRING OPTIONS(description='Event unique identifier')) \
                             PARTITION BY _PARTITIONDATE \
                             OPTIONS(partition_expiration_days=15)",
                "SELECT data.* FROM `$table_name` AS data \
                               LEFT JOIN $uploaded_table_name AS uploaded USING(uuid) \
                               WHERE uploaded.uuid IS NULL;"
                )
        if self._transactional_type == TransactionalType.GCLID_TIME:
            return self._ExecutionIntoBigQueryRequestTransactional(
                self._bq_ops_dataset,
                self._bq_location,
                "CREATE TABLE IF NOT EXISTS `$uploaded_table_name` ( \
                             timestamp TIMESTAMP OPTIONS(description= 'Event timestamp'), \
                             gclid STRING OPTIONS(description= 'Original gclid'), \
                             time STRING OPTIONS(description= 'Original time')) \
                             PARTITION BY _PARTITIONDATE \
                             OPTIONS(partition_expiration_days=15)",
                "SELECT data.* FROM `$table_name` AS data \
                               LEFT JOIN $uploaded_table_name AS uploaded USING(gclid, time) \
                               WHERE uploaded.gclid IS NULL;"
            )
        return self._ExecutionIntoBigQueryRequest(self._bq_location)

    def expand(self, executions):
        return (
            executions
            | beam.Filter(lambda execution: execution.destination.destination_type == self._destination_type)
            | beam.ParDo(self._get_bq_request_class())
            | beam.GroupByKey()
            | beam.ParDo(self._BatchElements(self._batch_size))
        )
