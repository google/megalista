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
import logging

from typing import Dict, Any, List

from uploaders.display_video.customer_match.abstract_uploader import DisplayVideoCustomerMatchAbstractUploaderDoFn
from uploaders import utils
from models.execution import DestinationType, AccountConfig


class DisplayVideoCustomerMatchContactInfoUploaderDoFn(DisplayVideoCustomerMatchAbstractUploaderDoFn):
  def get_list_definition(self, account_config: AccountConfig, destination_metadata: List[str], list_to_add: List[Dict[str, Any]]) -> Dict[str, Any]:
    list_name = destination_metadata[1]
    consent = self._get_consents(destination_metadata=destination_metadata)

    return {
      'displayName': list_name,
      'firstAndThirdPartyAudienceType': 'FIRST_AND_THIRD_PARTY_AUDIENCE_TYPE_FIRST_PARTY',
      'audienceType': 'CUSTOMER_MATCH_CONTACT_INFO',
      'membershipDurationDays': 10000,
      'description': 'List created automatically by Megalista',
      'contactInfoList': {
          'contactInfos': list_to_add,
          **(consent)
        }
    }

  def get_row_keys(self) -> List[str]:
    return ['hashedEmails', 'hashedPhoneNumbers', 'hashedFirstName', 'hashedLastName', 'countryCode', 'zipCodes']

  def get_action_type(self) -> DestinationType:
    return DestinationType.DV_CUSTOMER_MATCH_CONTACT_INFO_UPLOAD

  def _get_consents(self, destination_metadata: List[str]) -> Dict[str, str]:
      if len(destination_metadata) >= 7:
          if (
              destination_metadata[5] is not None
              and destination_metadata[6] is not None
          ):
              return {
                  "consent": {
                      "adUserData": destination_metadata[5],
                      "adPersonalization": destination_metadata[6],
                  },
              }

      return {}
  
  def get_update_list_definition(self, account_config: AccountConfig, destination_metadata: List[str], list_to_add: List[Dict[str, Any]]) -> Dict[str, Any]:
    advertiser_id = destination_metadata[0]

    consent = self._get_consents(destination_metadata=destination_metadata)

    return {
      'advertiserId': advertiser_id,
      'addedContactInfoList': {
        'contactInfos': list_to_add,
        **(consent)
      }
    }
  
