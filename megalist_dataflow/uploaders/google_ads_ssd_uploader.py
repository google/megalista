# Copyright 2019 Google LLC
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

from uploaders import google_ads_utils as ads_utils
from uploaders import utils as utils
from utils.execution import DestinationType


class GoogleAdsSSDUploaderDoFn(beam.DoFn):

  def __init__(self, oauth_credentials, developer_token):
    super().__init__()
    self.oauth_credentials = oauth_credentials
    self.developer_token = developer_token
    self.active = True
    if self.developer_token is None:
      self.active = False

  def _get_ssd_service(self, customer_id):
    return ads_utils.get_ads_service('OfflineDataUploadService', 'v201809', self.oauth_credentials,
                                     self.developer_token.get(), customer_id)

  def start_bundle(self):
    pass

  @staticmethod
  def _assert_conversion_name_is_present(execution):
    destination = execution.destination.destination_metadata
    if len(destination) is not 2:
      raise ValueError('Missing destination information. Found {}'.format(len(destination)))

    if not destination[0]:
      raise ValueError('Missing destination information. Received {}'.format(str(destination)))

  @utils.safe_process(logger=logging.getLogger("megalista.GoogleAdsSSDUploader"))
  def process(self, elements, **kwargs):
    """
    Args:
       elements: List of dict with two elements: 'execution' and 'row'. All executions must be equal.
    """
    if not self.active:
      logging.getLogger("megalista.GoogleAdsSSDUploader").warning('Skipping upload to ads, parameters not configured.')
      return

    ads_utils.assert_elements_have_same_execution(elements)
    any_execution = elements[0]['execution']
    ads_utils.assert_right_type_action(any_execution, DestinationType.ADS_SSD_UPLOAD)
    self._assert_conversion_name_is_present(any_execution)

    ssd_service = self._get_ssd_service(any_execution.account_config._google_ads_account_id)
    rows = utils.extract_rows(elements)
    self._do_upload(ssd_service, any_execution.destination.destination_metadata[0], any_execution.destination.destination_metadata[1],
                    rows)


  @staticmethod
  def _do_upload(ssd_service, conversion_name, ssd_external_upload_id, rows):
    upload_data = [{
      'StoreSalesTransaction': {
        'userIdentifiers': [
          {
            'userIdentifierType': 'HASHED_EMAIL',
            'value': conversion['hashedEmail']
          }
        ],
        'transactionTime': ads_utils.format_date(conversion['time']),
        'transactionAmount': {
          'currencyCode': 'BRL',
          'money': {
            'microAmount': conversion['amount']
          }
        },
        'conversionName': conversion_name
      }
    } for conversion in rows]

    offline_data_upload = {
      'externalUploadId': ssd_external_upload_id,
      'offlineDataList': upload_data,
      'uploadType': 'STORE_SALES_UPLOAD_FIRST_PARTY',
      'uploadMetadata': {
        'StoreSalesUploadCommonMetadata': {
          'xsi_type': 'FirstPartyUploadMetadata',
          'loyaltyRate': 1.0,
          'transactionUploadRate': 1.0,
        }
      }
    }

    add_conversions_operation = {
      'operand': offline_data_upload,
      'operator': 'ADD'
    }
    ssd_service.mutate([add_conversions_operation])
