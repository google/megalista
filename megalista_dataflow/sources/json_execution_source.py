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

from apache_beam.options.value_provider import ValueProvider

from sources.base_bounded_source import BaseBoundedSource
from models.execution import Destination, DestinationType
from models.execution import Execution, AccountConfig
from models.execution import Source, SourceType
from models.json_config import JsonConfig

LOGGER_NAME = "megalista.JsonExecutionSource"

class JsonExecutionSource(BaseBoundedSource):
  """
  Read Execution data from a JSON file. The URL is set-up in the parameter "setup_json_url"
  """

  def __init__(self, json_config: JsonConfig, setup_json_url: ValueProvider):
    super().__init__()
    self._json_config = json_config
    self._setup_json_url = setup_json_url

  def _do_count(self):
    json_url = self._setup_json_url.get()
    json_data = self._json_config.parse_json_from_url(json_url)
    return len(json_data)

  def read(self, range_tracker):
    json_url = self._setup_json_url.get()
    logging.get_logger(LOGGER_NAME).info(f"Loading configuration JSON {json_url}...")

    json_data = self._json_config.parse_json_from_url(json_url)
    google_ads_id = self._json_config.get_value(json_data, "GoogleAdsAccountId")
    mcc_json = self._json_config.get_value(json_data, "GoogleAdsMCC")
    mcc = False if mcc_json is None else mcc_json
    app_id = self._json_config.get_value(json_data, "AppId")
    google_analytics_account_id = self._json_config.get_value(json_data, "GoogleAnalyticsAccountId")
    campaign_manager_profile_id = self._json_config.get_value(json_data, "CampaignManagerProfileId")
    
    if campaign_manager_profile_id is None:
      campaign_manager_profile_id = self._json_config.get_value(json_data, "CampaignManagerAccountId")
    
    account_config = AccountConfig(google_ads_id, mcc, google_analytics_account_id, campaign_manager_profile_id, app_id)
    logging.get_logger(LOGGER_NAME).info(f"Loaded: {account_config}")

    sources = self._read_sources(self._json_config, json_data)
    destinations = self._read_destination(self._json_config, json_data)

    schedules = self._json_config.get_value(json_data, "Connections")
    if schedules:
      for schedule in schedules:
        if schedule["Enabled"]:
          logging.get_logger(LOGGER_NAME).info(
            f"Executing step Source:{schedule['Source']} -> Destination:{schedule['Destination']}")
          yield Execution(account_config, sources[schedule["Source"]], destinations[schedule["Destination"]])
    else:
      logging.get_logger(LOGGER_NAME).warn("No schedules found!")

  @staticmethod
  def _read_sources(json_config, json_data):
    sources_list = json_config.get_value(json_data, "Sources")
    sources = {}
    if sources_list:
      for row in sources_list:
        # Create Sources using Name, Type, and Metadata (dataset, table)
        source = Source(row["Name"], SourceType[row["Type"]],
                        [row["Dataset"], row["Table"]])
        sources[source.source_name] = source
    else:
      logging.get_logger(LOGGER_NAME).warn("No sources found!")
    return sources

  @staticmethod
  def _read_destination(json_config, json_data):
    destinations_list = json_config.get_value(json_data, "Destinations")
    destinations = {}
    if destinations_list:
      for row in destinations_list:
        # Create Destinations using Name, Type, and Metadata
        destination = Destination(row["Name"], DestinationType[row["Type"]],
                                  row["Metadata"])
        destinations[destination.destination_name] = destination
    else:
      logging.get_logger(LOGGER_NAME).warn("No destinations found!")
    return destinations
