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

import logging

import apache_beam as beam
from uploaders import google_ads_utils as ads_utils
from uploaders import utils
from utils.execution import DestinationType


class GoogleAdsOfflineUploaderDoFn(beam.DoFn):

  def __init__(self, oauth_credentials, developer_token):
    super().__init__()
    self.oauth_credentials = oauth_credentials
    self.developer_token = developer_token
    self.active = self.developer_token is not None

  def _get_oc_service(self, customer_id):
    return ads_utils.get_ads_service('OfflineConversionFeedService', 'v201809',
                                     self.oauth_credentials,
                                     self.developer_token.get(), customer_id)

  def start_bundle(self):
    pass

  @staticmethod
  def _assert_conversion_name_is_present(execution):
    destination = execution.destination.destination_metadata
    if len(destination) != 1:
      raise ValueError('Missing destination information. Found {}'.format(
          len(destination)))

    if not destination[0]:
      raise ValueError('Missing destination information. Received {}'.format(
          str(destination)))

  @utils.safe_process(
      logger=logging.getLogger('megalista.GoogleAdsOfflineUploader'))
  def process(self, elements_batch, **kwargs):
    if not self.active:
      logging.getLogger().warning(
          'Skipping upload, parameters not configured.')
      return

    ads_utils.assert_elements_have_same_execution(elements_batch)
    any_execution = elements_batch[0]['execution']
    ads_utils.assert_right_type_action(any_execution,
                                       DestinationType.ADS_OFFLINE_CONVERSION)
    self._assert_conversion_name_is_present(any_execution)

    oc_service = self._get_oc_service(
        any_execution.account_config.google_ads_account_id)

    self._do_upload(oc_service,
                    any_execution.destination.destination_metadata[0],
                    utils.extract_rows(elements_batch))

  @staticmethod
  def _do_upload(oc_service, conversion_name, rows):
    logging.getLogger().warning('Uploading {} rows to Google Ads'.format(
        len(rows)))
    upload_data = [{
        'operator': 'ADD',
        'operand': {
            'conversionName': conversion_name,
            'conversionTime': ads_utils.format_date(conversion['time']),
            'conversionValue': conversion['amount'],
            'googleClickId': conversion['gclid']
        }
    } for conversion in rows]

    oc_service.mutate(upload_data)
