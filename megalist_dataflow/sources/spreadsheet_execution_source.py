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
from apache_beam.options.value_provider import ValueProvider

from sources.base_bounded_source import BaseBoundedSource
from utils.execution import Action
from utils.execution import Execution

from utils.execution import SourceType
from utils.sheets_config import SheetsConfig


class SpreadsheetExecutionSource(BaseBoundedSource):
  """
  Read Execution data from a sheet. The sheet id is set-up in the parameter "setup_sheet_id"
  """

  def __init__(
      self,
      sheets_config,  # type: SheetsConfig
      setup_sheet_id  # type: ValueProvider
  ):
    super().__init__()
    self._sheets_config = sheets_config
    self._setup_sheet_id = setup_sheet_id

  def _do_count(self):
    # TODO: really count the number of lines in the sheet
    return 3

  def read(self, range_tracker):
    sheet_id = self._setup_sheet_id.get()

    sources = self._read_sources(self._sheets_config, sheet_id)
    destinations = self._read_destination(self._sheets_config, sheet_id)

    schedules = \
      self._sheets_config.get_range(sheet_id, 'Schedules!A2:D')['values']

    for schedule in schedules:
      if schedule[0] == 'YES':
        source_metadata = sources[schedule[1]]
        destination_metadata = destinations[schedule[2]]
        yield Execution(schedule[1],
                        source_metadata['type'],
                        source_metadata['metadata'],
                        schedule[2],
                        destination_metadata['action'],
                        destination_metadata['metadata'])

  @staticmethod
  def _read_sources(sheets_config, sheet_id):
    range = sheets_config.get_range(sheet_id, 'Sources!A2:E')

    sources = {}
    for row in range['values']:
      sources[row[0]] = {'type': SourceType[row[1]], 'metadata': row[2:]}

    return sources

  @staticmethod
  def _read_destination(sheets_config, sheet_id):
    range = sheets_config.get_range(sheet_id, 'Destinations!A2:E')

    destinations = {}
    for row in range['values']:
      destinations[row[0]] = {'action': Action[row[1]], 'metadata': row[2:]}

    return destinations
