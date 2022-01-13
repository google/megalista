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
from models.options import DataflowOptions

from data_sources.data_source import DataSource

_BIGQUERY_PAGE_SIZE = 20000

_LOGGER_NAME = 'megalista.BatchesFromExecutions'



class BatchesFromExecutions(beam.PTransform):
    """
    Filter the received executions by the received action,
    load the data using the received source and group by that batch size and Execution.
    """


    class _ReadDataSource(beam.DoFn):
        def __init__(self, transactional: bool, dataflow_options: DataflowOptions, args: dict):
            super().__init__()
            self._transactional = transactional
            self._dataflow_options = dataflow_options
            self._args = args

        def process(self, execution: Execution) -> Iterable[Any]:
            dataSource = DataSource.get_data_source(
                execution.source.source_type, execution.destination.destination_type, 
                self._transactional, self._dataflow_options, self._args)
            return dataSource.retrieve_data(execution)

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
        dataflow_options: DataflowOptions,
        destination_type: DestinationType,
        batch_size: int = 5000,
        transactional: bool = False
    ):
        super().__init__()
        
        self._dataflow_options = dataflow_options
        self._destination_type = destination_type
        self._batch_size = batch_size
        self._transactional = transactional
        self._args = {}
        if self._dataflow_options.bq_ops_dataset:
            self._args['bq_ops_dataset'] = self._dataflow_options.bq_ops_dataset
        
    def expand(self, executions):
        return (
            executions
            | beam.Filter(lambda execution: execution.destination.destination_type == self._destination_type)
            | beam.ParDo(self._ReadDataSource(self._transactional, self._dataflow_options, self._args))
            | beam.GroupBy(lambda x: x['execution'])
            | beam.ParDo(self._BatchElements(self._batch_size))
        )
