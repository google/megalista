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
import distutils.util
import logging

from apache_beam.options.value_provider import ValueProvider

from sources.base_bounded_source import BaseBoundedSource
from models.execution import Destination, DestinationType
from models.execution import Execution, AccountConfig
from models.execution import Source, SourceType
from models.sheets_config import SheetsConfig
from uploaders import utils

class SpreadsheetExecutionSource(BaseBoundedSource):
  """
  Read Execution data from a sheet. The sheet id is set-up in the parameter "setup_sheet_id"
  """

  def __init__(
      self,
      sheets_config: SheetsConfig,
      setup_sheet_id: ValueProvider
  ):
    super().__init__()
    self._sheets_config = sheets_config
    self._setup_sheet_id = setup_sheet_id

  def _do_count(self):
    # TODO: really count the number of lines in the sheet
    return 3

  def read(self, range_tracker):
    sheet_id = self._setup_sheet_id.get()
    logging.getLogger("megalista.SpreadsheetExecutionSource").info(f"Loading configuration sheet {sheet_id}...")
    google_ads_id = self._sheets_config.get_value(sheet_id, "GoogleAdsAccountId")
    mcc_trix = self._sheets_config.get_value(sheet_id, "GoogleAdsMCC")
    mcc = False if mcc_trix is None else bool(distutils.util.strtobool(mcc_trix))
    app_id = self._sheets_config.get_value(sheet_id, "AppId")
    google_analytics_account_id = self._sheets_config.get_value(sheet_id, "GoogleAnalyticsAccountId")

    if self._sheets_config.check_if_range_exists(sheet_id, "CampaignManagerProfileId"):
      campaign_manager_profile_id = self._sheets_config.get_value(sheet_id, "CampaignManagerProfileId")
    else:
      campaign_manager_profile_id = self._sheets_config.get_value(sheet_id, "CampaignManagerAccountId")
        
    if google_ads_id is not None:
      google_ads_id = utils.clean_ads_customer_id(google_ads_id)
      
    account_config = AccountConfig(google_ads_id, mcc, google_analytics_account_id, campaign_manager_profile_id, app_id)
    logging.getLogger("megalista.SpreadsheetExecutionSource").info(f"Loaded: {account_config}")

    sources = self._read_sources(self._sheets_config, sheet_id)
    destinations = self._read_destination(self._sheets_config, sheet_id)

    schedules_range = self._sheets_config.get_range(sheet_id, 'SchedulesRange')
    if 'values' in schedules_range:
      for schedule in schedules_range['values']:
        if schedule[0] == 'YES':
          logging.getLogger("megalista.SpreadsheetExecutionSource").info(
            f"Executing step Source:{sources[schedule[1]].source_name} -> Destination:{destinations[schedule[2]].destination_name}")
          yield Execution(account_config, sources[schedule[1]], destinations[schedule[2]])
    else:
      logging.getLogger("megalista.SpreadsheetExecutionSource").warn("No schedules found!")

  @staticmethod
  def _read_sources(sheets_config, sheet_id):
    range = sheets_config.get_range(sheet_id, 'SourcesRange')
    sources = {}
    if 'values' in range:
      for row in range['values']:
        source = Source(row[0], SourceType[row[1]], row[2:])
        sources[source.source_name] = source
    else:
      logging.getLogger("megalista.SpreadsheetExecutionSource").warn("No sources found!")
    return sources

  @staticmethod
  def _read_destination(sheets_config, sheet_id):
    range = sheets_config.get_range(sheet_id, 'DestinationsRange')
    destinations = {}
    if 'values' in range:
      for row in range['values']:
        destination = Destination(row[0], DestinationType[row[1]], row[2:])
        destinations[destination.destination_name] = destination
    else:
      logging.getLogger("megalista.SpreadsheetExecutionSource").warn("No destinations found!")
    return destinations
