# Copyright 2022 Google LLC
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

import apache_beam as beam

from error.error_handling import ErrorHandler
from models.execution import Execution


class MegalistaUploader(beam.DoFn):
  """
  General DoFn to be used as supper for DoFn uploaders.
  Add error notification capabilities.
  """

  def __init__(self, error_handler: ErrorHandler):
    super().__init__()
    self._error_handler = error_handler

  def _add_error(self, execution: Execution, error_message: str):
    self._error_handler.add_error(execution, error_message)

  def teardown(self):
    self._error_handler.notify_errors()
