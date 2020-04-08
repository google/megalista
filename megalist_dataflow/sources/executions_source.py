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


class ExecutionsSource(BaseBoundedSource):
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
    return 3

  def read(self, range_tracker):
    sources = self._read_sources(self._sheets_config)
    destinations = self._read_destination(self._sheets_config)

    schedules = \
      self._sheets_config.get_range(self._setup_sheet_id.get(), 'Schedules!A2:D')['values']

    for schedule in schedules:
      if schedule[0] == 'YES':
        source_metadata = sources[schedule[1]]
        action = Action[schedule[2]]
        destination_metadata = destinations[schedule[3]]
        yield Execution(schedule[1],
                        source_metadata['type'],
                        source_metadata['metadata'],
                        action,
                        schedule[3],
                        destination_metadata)

  @staticmethod
  def _read_sources(sheets_config):
    range = sheets_config.get_range('1Z0SC50DUc-w60ERkF1T4ZdXZibwg2r1sYtw_YMAiGeM', 'Sources!A2:E')

    sources = {}
    for row in range['values']:
      sources[row[0]] = {'type': SourceType[row[1]], 'metadata': row[2:]}

    return sources

  @staticmethod
  def _read_destination(sheets_config):
    range = sheets_config.get_range('1Z0SC50DUc-w60ERkF1T4ZdXZibwg2r1sYtw_YMAiGeM', 'Destinations!A2:D')

    destinations = {}
    for row in range['values']:
      destinations[row[0]] = row[1:]

    return destinations
