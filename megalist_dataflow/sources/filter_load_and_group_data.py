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

from apache_beam import PTransform, DoFn

import apache_beam as beam

from sources.bq_api_dofn import BigQueryApiDoFn
from utils.execution import Action
from utils.group_by_execution_dofn import GroupByExecutionDoFn


def filter_by_action(execution, action):
  return execution.action is action


class FilterLoadAndGroupData(PTransform):
  """
  Filter the received executions by the received action,
  load the data using the received source and group by that batch size and Execution.
  """

  def __init__(
      self,
      action  # type: Action
  ):
    super().__init__()
    self._source_dofn = BigQueryApiDoFn()
    self._action = action

  def expand(self, input_or_inputs):
    # todo: rotear a source baseado no tipo presente na Execution
    return input_or_inputs | \
           beam.Filter(filter_by_action, self._action) | \
           beam.ParDo(self._source_dofn) | \
           beam.ParDo(GroupByExecutionDoFn())
