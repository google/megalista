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

import apache_beam as beam
import logging
import pytz
import datetime

from uploaders import google_ads_utils as utils
from utils.execution import Action

timezone = pytz.timezone('America/Sao_Paulo')


class GoogleAdsOfflineUploaderDoFn(beam.DoFn):
  def __init__(self, oauth_credentials, developer_token, customer_id):
    super().__init__()
    self.oauth_credentials = oauth_credentials
    self.developer_token = developer_token
    self.customer_id = customer_id
    self.active = True
    if self.developer_token is None or self.customer_id is None:
      self.active = False

  def _get_oc_service(self):
    return utils.get_ads_service('OfflineConversionFeedService', 'v201809', self.oauth_credentials,
                                 self.developer_token.get(), self.customer_id.get())

  @staticmethod
  def _format_date(date):
    pdate = datetime.datetime.strptime(date, "%Y-%m-%dT%H:%M:%S.%f")
    return '%s %s' % (datetime.datetime.strftime(pdate, '%Y%m%d %H%M%S'), timezone.zone)

  def start_bundle(self):
    pass

  @staticmethod
  def _assert_convertion_name_is_present(execution):
    destination = execution.destination_metadata
    if len(destination) is not 1:
      raise ValueError('Missing destination information. Found {}'.format(len(destination)))

    if not destination[0]:
      raise ValueError('Missing destination information. Received {}'.format(str(destination)))

  def process(self, elements_batch, **kwargs):
    if not self.active:
      logging.getLogger().warning("Skipping upload to ads, parameters not configured.")
      return

    if len(elements_batch) == 0:
      logging.getLogger().warning('Skipping upload to ads, received no elements.')
      return

    utils.assert_elements_have_same_execution(elements_batch)
    any_execution = elements_batch[0]['execution']
    utils.assert_right_type_action(any_execution, Action.ADS_OFFLINE_CONVERSION)
    self._assert_convertion_name_is_present(any_execution)

    oc_service = self._get_oc_service()

    self._do_upload(oc_service, any_execution.destination_metadata[0], self._extract_rows(elements_batch))

  @staticmethod
  def _extract_rows(elements):
    return [dict['row'] for dict in elements]

  @staticmethod
  def _do_upload(oc_service, conversion_name, rows):
    logging.getLogger().warning('Uploading {} rows to Google Ads'.format(len(rows)))
    upload_data = [
      {
        'operator': 'ADD',
        'operand': {
          'conversionName': conversion_name,
          'conversionTime': GoogleAdsOfflineUploaderDoFn._format_date(conversion['time']),
          'conversionValue': conversion['amount'],
          'googleClickId': conversion['gclid']
        }
      } for conversion in rows]

    oc_service.mutate(upload_data)
