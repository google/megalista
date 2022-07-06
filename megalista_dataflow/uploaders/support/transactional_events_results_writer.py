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

from uploaders import utils
from models.execution import Batch
from models.options import DataflowOptions
from data_sources.data_source import DataSource
from models.execution import TransactionalType
from uploaders import MegalistaUploader

class TransactionalEventsResultsWriter(MegalistaUploader):
  """
  Uploads UUIDs from rows successfully sent by the uploader.
  It uploads the rows to a table with the same name of the source table plus the suffix '_uploaded'.
  """

  def __init__(self, dataflow_options: DataflowOptions, transactional_type: TransactionalType):
    super().__init__()
    self._dataflow_options = dataflow_options
    self._transactional_type = transactional_type
    
  @utils.safe_process(logger=logging.getLogger("megalista.TransactionalEventsResultsWriter"))
  def process(self, batch: Batch, *args, **kwargs):
    self._do_process(batch)

  def _do_process(self, batch: Batch):
    execution = batch.execution

    data_source = DataSource.get_data_source(
      execution.source.source_type, execution.source.source_name,
      execution.destination.destination_type, execution.destination.destination_name, 
      self._transactional_type, self._dataflow_options)
    return data_source.write_transactional_info(batch.elements, execution)
    