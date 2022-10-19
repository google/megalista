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
import functools

from apache_beam.coders import coders
from apache_beam.options.value_provider import ValueProvider
from google.cloud import bigquery
from error.error_handling import ErrorHandler
from uploaders.uploaders import MegalistaUploader
from models.execution import DestinationType, Execution, Batch, TransactionalType, ExecutionsGroupedBySource, DataRowsGroupedBySource
from string import Template
from typing import Any, List, Iterable, Tuple, Dict, Optional
from models.options import DataflowOptions
from data_sources.data_source import DataSource


_BIGQUERY_PAGE_SIZE = 20000

_LOGGER_NAME = 'megalista.BatchesFromExecutions'

# max int size. 
# used for avoiding overflow when casting from str to int (underlying C code)
_INT_MAX = 2147483647 

class ExecutionCoder(coders.Coder):
    """A custom coder for the Execution class."""

    def encode(self, o):
        return json.dumps(o.to_dict()).encode('utf-8')

    def decode(self, s):
        return Execution.from_dict(json.loads(s.decode('utf-8')))

    def is_deterministic(self):
        return True

class ExecutionsGroupedBySourceCoder(coders.Coder):
    """A custom coder for the Execution class."""

    def encode(self, o):
        return json.dumps(o.to_dict()).encode('utf-8')

    def decode(self, s):
        return ExecutionsGroupedBySource.from_dict(json.loads(s.decode('utf-8')))

    def is_deterministic(self):
        return True


class DataRowsGroupedBySourceCoder(coders.Coder):
    """A custom coder for the Execution class."""

    def encode(self, o):
        return json.dumps(o.to_dict()).encode('utf-8')

    def decode(self, s):
        return DataRowsGroupedBySource.from_dict(json.loads(s.decode('utf-8')))

    def is_deterministic(self):
        return True

    def estimate_size(self, o):
        """Estimation of P-Collection size (in bytes).
        - Called from Dataflow / Apache Beam
        - Estimated size had to be truncated into _INT_MAX for 
        avoiding overflow when casting from str to int
        (in C underlying code)."""
        amount_of_rows = len(o.rows)
        row_size = 0
        if amount_of_rows > 0:
            row_size = len(json.dumps(o.rows[0]).encode('utf-8'))
        estimate = amount_of_rows * row_size
        # there is an overflow error if estimated size > _INT_MAX
        if estimate > _INT_MAX:
            estimate = _INT_MAX
        return estimate



class BatchesFromExecutions(beam.PTransform):
    """
    Filter the received executions by the received action,
    load the data using the received source and group by that batch size and Execution.
    """

    class _ReadDataSource(MegalistaUploader):
        def __init__(self, transactional_type: TransactionalType, dataflow_options: DataflowOptions, error_handler: ErrorHandler):
            super().__init__(error_handler)
            self._transactional_type = transactional_type
            self._dataflow_options = dataflow_options

        def process(self, executions: ExecutionsGroupedBySource) -> Iterable[DataRowsGroupedBySource]:
            data_source = DataSource.get_data_source(
                executions, self._transactional_type, self._dataflow_options)
            return data_source.retrieve_data(executions)

    class _BatchElements(MegalistaUploader):
        def __init__(self, batch_size: int, error_handler: ErrorHandler):
            super().__init__(error_handler)
            self._batch_size = batch_size

        def process(self, grouped_elements):
            # grouped_elements[0] is the grouping key, the execution
            execution = grouped_elements[0]
            batch: List[Any] = []        
            # Keeps track of the batch iteration
            iteration = 1
            # grouped_elements[1] is the list of elements
            for i, element in enumerate(grouped_elements[1]):
                if i != 0 and i % self._batch_size == 0:
                    yield Batch(execution, batch, iteration)
                    iteration += 1
                    batch = []
                batch.append(element)
            yield Batch(execution, batch, iteration)

    class _BreakIntoExecutions(beam.DoFn):
        def __init__(self, destination_type: DestinationType):
            self._destination_type = destination_type

        def process(self, el):
            for item in el:
                if item[0].destination.destination_type == self._destination_type:
                    yield item

    def __init__(
        self,
        error_handler: ErrorHandler,
        dataflow_options: DataflowOptions,
        destination_type: DestinationType,
        batch_size: int = 5000,
        transactional_type: TransactionalType = TransactionalType.NOT_TRANSACTIONAL
    ):
        super().__init__()
        
        self._error_handler = error_handler
        self._dataflow_options = dataflow_options
        self._destination_type = destination_type
        self._batch_size = batch_size
        self._transactional_type = transactional_type

    def expand(self, executions):
        return (
            executions
            | beam.Filter(
                lambda executionGroupedBySource: functools.reduce(
                    lambda a, b: a or b,
                    [execution.destination.destination_type == self._destination_type for execution in executionGroupedBySource.executions],
                    False
                )
            )
            | beam.ParDo(self._ReadDataSource(self._transactional_type, self._dataflow_options, self._error_handler))
            | beam.Map(lambda el: [(execution, el.rows) for execution in iter(el.executions.executions)])
            | beam.ParDo(self._BreakIntoExecutions(self._destination_type))
            | beam.ParDo(self._BatchElements(self._batch_size, self._error_handler))
        )
