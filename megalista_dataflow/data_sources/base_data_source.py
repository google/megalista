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
from models.execution import Execution, ExecutionsGroupedBySource, TransactionalType, DataRowsGroupedBySource
from typing import Any, Iterable, List
from apache_beam.typehints.decorators import with_output_types

class BaseDataSource:
  def __init__(self, executions: ExecutionsGroupedBySource, transactional_type: TransactionalType):
    self._executions = executions
    self._transactional_type = transactional_type

    self._source_type = executions.source.source_type
    self._source_name = executions.source.source_name
    self._destination_type = executions[0].destination.destination_type
    self._destination_name = executions[0].destination.destination_name

  #@with_output_types(DataRowsGroupedBy.Source)
  def retrieve_data(self, executions: ExecutionsGroupedBySource) -> List[DataRowsGroupedBySource]:
    raise NotImplementedError("Source Type not implemented. Please check your configuration (sheet / json / firestore).")
  
  def write_transactional_info(self, rows, execution):
    raise NotImplementedError("Source Type not implemented. Please check your configuration (sheet / json / firestore).")
  
  @staticmethod
  def _convert_row_to_dict(row):
    result = {}
    for key, value in row.items():
        result[key] = value
    return result
 