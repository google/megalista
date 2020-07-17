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

import time
import math
import logging
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
import apache_beam as beam

from uploaders import google_ads_utils as ads_utils
from uploaders import utils as utils
from utils.execution import DestinationType


class CampaignManagerConversionUploaderDoFn(beam.DoFn):
  def __init__(self, oauth_credentials):
    super().__init__()
    self.oauth_credentials = oauth_credentials
    self.active = True

  def _get_dcm_service(self):
    credentials = Credentials(
      token=self.oauth_credentials.get_access_token(),
      refresh_token=self.oauth_credentials.get_refresh_token(),
      client_id=self.oauth_credentials.get_client_id(),
      client_secret=self.oauth_credentials.get_client_secret(),
      token_uri='https://accounts.google.com/o/oauth2/token',
      scopes=['https://www.googleapis.com/auth/dfareporting',
              'https://www.googleapis.com/auth/dfatrafficking',
              'https://www.googleapis.com/auth/ddmconversions'])

    return build('dfareporting', 'v3.3', credentials=credentials)

  def start_bundle(self):
    pass

  @staticmethod
  def _assert_all_list_names_are_present(any_execution):
    destination = any_execution.destination.destination_metadata
    if len(destination) != 2:
      raise ValueError('Missing destination information. Found {}'.format(len(destination)))

    if not destination[0] \
        or not destination[1]:
      raise ValueError('Missing destination information. Received {}'.format(str(destination)))

  @utils.safe_process(logger=logging.getLogger("megalista.CampaignManagerConversionUploader"))
  def process(self, elements, **kwargs):
    self._do_process(elements, time.time())

  def _do_process(self, elements, time):
    if not self.active:
      logging.getLogger("megalista.CampaignManagerConversionsUploader").warning("Skipping upload to Campaign Manager, parameters not configured.")
      return

    ads_utils.assert_elements_have_same_execution(elements)
    any_execution = elements[0]['execution']
    ads_utils.assert_right_type_action(any_execution, DestinationType.CM_OFFLINE_CONVERSION)
    self._assert_all_list_names_are_present(any_execution)

    floodlight_activity_id = any_execution.destination.destination_metadata[0]
    floodlight_configuration_id = any_execution.destination.destination_metadata[1]

    self._do_upload_data(floodlight_activity_id, floodlight_configuration_id, any_execution.account_config.campaign_manager_account_id, time, utils.extract_rows(elements))

  def _do_upload_data(self, floodlight_activity_id, floodlight_configuration_id, campaign_manager_account_id, time, rows):

    service = self._get_dcm_service()
    conversions = []
    for conversion in rows:
      to_upload = {
        'floodlightActivityId': floodlight_activity_id,
        'floodlightConfigurationId': floodlight_configuration_id,
        'ordinal': math.floor(time * 10e5),
        'timestampMicros': math.floor(time * 10e5)
      }
      logging.getLogger("megalista.CampaignManagerConversionsUploader").info(conversion)
      if 'gclid' in conversion and conversion['gclid']:
        to_upload['gclid'] = conversion['gclid']
      elif 'encryptedUserId' in conversion and conversion['encryptedUserId']:
        to_upload['encryptedUserId'] = conversion['encryptedUserId']
      elif 'mobileDeviceId' in conversion and conversion['mobileDeviceId']:
        to_upload['mobileDeviceId'] = conversion['mobileDeviceId']
      logging.getLogger("megalista.CampaignManagerConversionsUploader").info(to_upload)
      conversions.append(to_upload)

    request_body = {
      'conversions': conversions,
      'encryptionInfo': 'AD_SERVING'
    }
    request = service.conversions().batchinsert(profileId=campaign_manager_account_id, body=request_body)
    response = request.execute()
    if response['hasFailures']:
      logging.getLogger("megalista.CampaignManagerConversionsUploader").error('Error(s) inserting conversions:')
      status = response['status'][0]
      for error in status['errors']:
        logging.getLogger("megalista.CampaignManagerConversionsUploader").error('\t[%s]: %s' % (
          error['code'], error['message']))
      logging.getLogger("megalista.CampaignManagerConversionsUploader").error(response)
