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

from functools import reduce
import apache_beam as beam
import logging

from typing import List, Dict, Any

from uploaders.display_video.customer_match.abstract_uploader import DisplayVideoCustomerMatchAbstractUploaderDoFn
from uploaders import utils as utils
from models.execution import DestinationType, AccountConfig


class DisplayVideoCustomerMatchMobileUploaderDoFn(DisplayVideoCustomerMatchAbstractUploaderDoFn):
  def get_list_definition(self, account_config: AccountConfig, destination_metadata: List[str], list_to_add: List[Dict[str, Any]]) -> Dict[str, Any]:
    list_name = destination_metadata[1]    
    app_id = account_config.app_id
    
    #overwrite app_id from default to custom
    if len(destination_metadata) >=4 and destination_metadata[3]:
      app_id = destination_metadata[3]
      
    return {
      'displayName': list_name,
      'firstAndThirdPartyAudienceType': 'FIRST_AND_THIRD_PARTY_AUDIENCE_TYPE_FIRST_PARTY',
      'audienceType': 'CUSTOMER_MATCH_DEVICE_ID',
      'membershipDurationDays': 10000,
      'description': 'List created automatically by Megalista',
      'appId': app_id,
      'mobileDeviceIdList': {
        'mobileDeviceIds': self.get_simplified_list(list_to_add)
      }
    }

  def get_row_keys(self) -> List[str]:
    return ['mobileDeviceIds']

  def get_action_type(self) -> DestinationType:
    return DestinationType.DV_CUSTOMER_MATCH_DEVICE_ID_UPLOAD

  @staticmethod
  def _add_item_into_simplified_list(accumulator: list, item: Any) -> list:
    mobile_device_ids = []
    if type(item['mobileDeviceIds']) is list:
      mobile_device_ids = item['mobileDeviceIds']
    else:
      mobile_device_ids = [item['mobileDeviceIds']]
    return accumulator + mobile_device_ids

  def get_simplified_list(self, list_to_add: List[Dict[str, Any]]) -> List[str]:
    # Alls devices must be within the same array
    # a single (Str) device id may be passed, so could a list od devices
    # Therefore, normalizes it within a single, one dimension list
    return list(
        reduce(
          DisplayVideoCustomerMatchMobileUploaderDoFn._add_item_into_simplified_list,
          list_to_add, 
          []
        )
      )

  def get_update_list_definition(self, account_config: AccountConfig, destination_metadata: List[str], list_to_add: List[Dict[str, Any]]) -> Dict[str, Any]:
      advertiser_id = destination_metadata[0]

      return {
          'advertiserId': advertiser_id,
          'addedMobileDeviceIdList': {
              'mobileDeviceIds': self.get_simplified_list(list_to_add)
          }
      }