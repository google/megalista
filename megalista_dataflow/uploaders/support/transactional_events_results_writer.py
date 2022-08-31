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

from config import logging
from datetime import datetime

import apache_beam as beam
from mappers.batches_grouped_by_source_mapper import BatchesGroupedBySourceCombineFn, BatchesGroupedBySourceMapper

from uploaders import utils
from models.execution import Batch, BatchesGroupedBySource, ExecutionsGroupedBySource
from models.options import DataflowOptions
from data_sources.data_source import DataSource
from models.execution import TransactionalType
from uploaders.uploaders import MegalistaUploader
from error.error_handling import ErrorHandler

class TransactionalEventsResultsWriter(beam.PTransform):
  """
  Uploads UUIDs from rows successfully sent by the uploader.
  It uploads the rows to a table with the same name of the source table plus the suffix '_uploaded'.
  """

  class _UploadData(MegalistaUploader):
    def __init__(self, dataflow_options: DataflowOptions, transactional_type: TransactionalType, error_handler: ErrorHandler):
      super().__init__(error_handler)
      self._dataflow_options = dataflow_options
      self._transactional_type = transactional_type
      
    @utils.safe_process(logger=logging.get_logger("megalista.TransactionalEventsResultsWriter"))
    def process(self, batch: Batch, *args, **kwargs):
      self._do_process(batch)
      return [batch.execution]

    def _do_process(self, batch: Batch):
      executions = ExecutionsGroupedBySource(batch.execution.source.source_name, [batch.execution])

      data_source = DataSource.get_data_source(
        executions, self._transactional_type, self._dataflow_options)
      return data_source.write_transactional_info(batch.elements, batch.execution)
  

  class _ElementsProcessor(MegalistaUploader):
    def __init__(self, error_handler: ErrorHandler):
      super().__init__(error_handler)
      
    def process(self, batches: BatchesGroupedBySource):
      elements = []
      for batch in batches.batches:
        for el in batch.elements:
          if el not in elements:
            elements.append(el)
      
      return [Batch(batches.batches[0].execution, elements)]

  def __init__(self, dataflow_options: DataflowOptions, transactional_type: TransactionalType, error_handler: ErrorHandler):
    self._dataflow_options = dataflow_options
    self._transactional_type = transactional_type
    self._error_handler = error_handler

  def expand(self, executions):
    return (
        executions
        | "Transform into tuples" >> beam.Map(lambda batch: (batch.execution.source.source_name, batch))
        | "Group by source name" >> beam.CombinePerKey(BatchesGroupedBySourceCombineFn())
        | "Encapsulate into object" >> beam.Map(BatchesGroupedBySourceMapper().encapsulate)
        | beam.ParDo(self._ElementsProcessor(self._error_handler))
        | beam.ParDo(self._UploadData(self._dataflow_options, self._transactional_type, self._error_handler))
    )