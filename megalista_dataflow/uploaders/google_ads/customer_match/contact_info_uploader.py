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

from uploaders.google_ads.customer_match.abstract_uploader import GoogleAdsCustomerMatchAbstractUploaderDoFn
from uploaders import utils
from models.execution import DestinationType, AccountConfig


class GoogleAdsCustomerMatchContactInfoUploaderDoFn(GoogleAdsCustomerMatchAbstractUploaderDoFn):
  def get_list_definition(self, account_config: AccountConfig, destination_metadata: List[str]) -> Dict[str, Any]:
    list_name = destination_metadata[0]
    return {
      'membership_status': 'OPEN',
      'name': list_name,
      'description': 'List created automatically by Megalista',
      'membership_life_span': 10000,
      'crm_based_user_list': {
        'upload_key_type': 'CONTACT_INFO', #CONTACT_INFO, CRM_ID, MOBILE_ADVERTISING_ID
        'data_source_type': 'FIRST_PARTY',
      }
    }

  def get_row_keys(self) -> List[str]:
    return ['hashed_email', 'address_info', 'hashed_phone_number']

  def get_action_type(self) -> DestinationType:
    return DestinationType.ADS_CUSTOMER_MATCH_CONTACT_INFO_UPLOAD
