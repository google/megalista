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

from typing import Any, Dict, List, Iterable, Tuple

import apache_beam as beam
import logging
from google.cloud import bigquery
from apache_beam.io.gcp.bigquery import ReadFromBigQueryRequest

from models.execution import DestinationType, Execution, Batch


class BatchesFromExecutions(beam.PTransform):
    """
    Filter the received executions by the received action,
    load the data using the received source and group by that batch size and Execution.
    """

    class _ExecutionIntoBigQueryRequest(beam.DoFn):
        def process(self, execution: Execution) -> Iterable[ReadFromBigQueryRequest]:
            table_name = execution.source.source_metadata[0] + \
                '.' + execution.source.source_metadata[1]
            query = f"SELECT Data.*, '{hash(execution)}' AS execution_hash FROM {table_name} AS Data"
            return [ReadFromBigQueryRequest(query=query)]

    class _ExecutionIntoBigQueryRequestTransactional(beam.DoFn):
        def process(self, execution: Execution) -> Iterable[ReadFromBigQueryRequest]:
            table_name = execution.source.source_metadata[0] + \
                '.' + execution.source.source_metadata[1]
            uploaded_table_name = f"{table_name}_uploaded"
            client = bigquery.Client()

            query = "CREATE TABLE IF NOT EXISTS " + uploaded_table_name + " ( \
              timestamp TIMESTAMP OPTIONS(description= 'Event timestamp'), \
              uuid STRING OPTIONS(description='Event unique identifier'))\
              PARTITION BY _PARTITIONDATE \
              OPTIONS(partition_expiration_days=15)"

            logging.getLogger("megalista.ExecutionIntoBigQueryRequestTransactional").info(
                "Creating table %s if it doesn't exist", uploaded_table_name)

            client.query(query).result()

            query = f"SELECT Data.*, '{hash(execution)}' AS execution_hash FROM {table_name} AS Data \
                LEFT JOIN {uploaded_table_name} AS Uploaded USING(uuid) \
                WHERE Uploaded.uuid IS NULL;"

            return [ReadFromBigQueryRequest(query=query)]

    class _BatchElements(beam.DoFn):
        def __init__(self, batch_size: int):
            self._batch_size = batch_size

        def process(self, element, executions: Iterable[Execution]):
            execution = next(
                (execution for execution in executions if str(hash(execution)) == element[0]))
            batch: List[Any] = []
            for i, element in enumerate(element[1]):
                if i != 0 and i % self._batch_size == 0:
                    yield Batch(execution, batch)
                    batch = []
                batch.append(element)
            yield Batch(execution, batch)

    def __init__(
        self,
        destination_type: DestinationType,
        batch_size: int = 5000,
        transactional: bool = False
    ):
        super().__init__()
        self._destination_type = destination_type
        self._batch_size = batch_size
        self._transactional = transactional

    def _get_bq_request_class(self):
        if self._transactional:
            return self._ExecutionIntoBigQueryRequestTransactional()
        return self._ExecutionIntoBigQueryRequest()

    def expand(self, executions):
        return (
            executions
            | beam.Filter(lambda execution: execution.destination.destination_type == self._destination_type)
            | beam.ParDo(self._get_bq_request_class())
            | beam.io.ReadAllFromBigQuery()
            | beam.GroupBy(lambda x: x['execution_hash'])
            | beam.ParDo(self._BatchElements(self._batch_size), beam.pvalue.AsList(executions))
        )
