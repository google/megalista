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

import apache_beam as beam
from config import logging

from typing import Dict, Any, List

from uploaders.display_video.customer_match.abstract_uploader import DisplayVideoCustomerMatchAbstractUploaderDoFn
from uploaders import utils
from models.execution import DestinationType, AccountConfig


class DisplayVideoCustomerMatchContactInfoUploaderDoFn(DisplayVideoCustomerMatchAbstractUploaderDoFn):
  def get_list_definition(self, account_config: AccountConfig, destination_metadata: List[str], list_to_add: List[Dict[str, Any]]) -> Dict[str, Any]:
    list_name = destination_metadata[1]
    return {
      'displayName': list_name,
      'firstAndThirdPartyAudienceType': 'FIRST_AND_THIRD_PARTY_AUDIENCE_TYPE_FIRST_PARTY',
      'audienceType': 'CUSTOMER_MATCH_CONTACT_INFO',
      'membershipDurationDays': 10000,
      'description': 'List created automatically by Megalista',
      'contactInfoList': {
          'contactInfos': list_to_add
        }
    }

  def get_row_keys(self) -> List[str]:
    return ['hashedEmails', 'hashedPhoneNumbers', 'hashedFirstName', 'hashedLastName', 'countryCode', 'zipCodes']

  def get_action_type(self) -> DestinationType:
    return DestinationType.DV_CUSTOMER_MATCH_CONTACT_INFO_UPLOAD

  def get_update_list_definition(self, account_config: AccountConfig, destination_metadata: List[str], list_to_add: List[Dict[str, Any]]) -> Dict[str, Any]:
    advertiser_id = destination_metadata[0]

    return {
      'advertiserId': advertiser_id,
      'addedContactInfoList': {
        'contactInfos': list_to_add
      }
    }
