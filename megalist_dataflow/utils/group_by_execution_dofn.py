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

from apache_beam import DoFn


class GroupByExecutionDoFn(DoFn):
  """
  Group elements received in batches.
  Elements must by tuples were [0] is and Execution.
  When an Execution changes between elements, a batch is returned if it hasn't archived batch size
  """

  def __init__(self,
      batch_size=5000  # type: int
  ):
    super().__init__()
    self._batch_size = batch_size
    self._batch = None
    self._last_execution = None

  def start_bundle(self):
    self._batch = []

  def process(self, element, *args, **kwargs):
    execution = element[0]
    if self._last_execution is not None and self._last_execution != execution:
      yield self._batch
      self._batch = []

    self._last_execution = execution

    self._batch.append(element)
    if len(self._batch) >= self._batch_size:
      yield self._batch
      self._batch = []
