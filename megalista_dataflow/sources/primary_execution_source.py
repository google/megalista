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

from apache_beam.options.value_provider import ValueProvider

from sources.base_bounded_source import BaseBoundedSource
from sources.spreadsheet_execution_source import SpreadsheetExecutionSource
from sources.firestore_execution_source import FirestoreExecutionSource
from sources.json_execution_source import JsonExecutionSource
from models.execution import Destination, DestinationType
from models.execution import Execution, AccountConfig
from models.execution import Source, SourceType
from models.json_config import JsonConfig
from models.sheets import SheetsConfig


class PrimaryExecutionSource(BaseBoundedSource):
  """
  Determines execution data based on provided JSON or Sheet information. Control
  class needed since variables can't be accessed until runtime.
  """

  def __init__(self,
        sheets_config: SheetsConfig,
        json_config: JsonConfig,
        setup_sheet_id: ValueProvider,
        setup_json_url: ValueProvider,
        setup_firestore_collection: ValueProvider):
    super().__init__()
    self._setup_sheet_id = setup_sheet_id
    self._setup_json_url = setup_json_url
    self._setup_firestore_collection = setup_firestore_collection
    self._sheets_execution_source = SpreadsheetExecutionSource(sheets_config,
                                                               setup_sheet_id)
    self._json_execution_source = JsonExecutionSource(json_config,
                                                      setup_json_url)
    self._firestore_execution_source = FirestoreExecutionSource(setup_firestore_collection)

  def _do_count(self):
    if self._setup_sheet_id.get():
      logging.getLogger("megalista").info("Using Sheets count")
      return self._sheets_execution_source._do_count()
    elif self._setup_firestore_collection.get():
      logging.getLogger("megalista").info("Using Firestore count")
      return self._firestore_execution_source._do_count()
    else:
      logging.getLogger("megalista").info("Using JSON count")
      return self._json_execution_source._do_count()

  def read(self, range_tracker):
    if self._setup_sheet_id.get():
      logging.getLogger("megalista").info("Reading Sheets configuration")
      return self._sheets_execution_source.read(range_tracker)
    elif self._setup_firestore_collection.get():
      logging.getLogger("megalista").info("Reading Firestore configuration")
      return self._firestore_execution_source.read(range_tracker)
    else:
      logging.getLogger("megalista").info("Reading JSON configuration")
      return self._json_execution_source.read(range_tracker)
      