"""Campaign Manager Conversion Uploader beam module."""
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
import math
import time

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

from error.error_handling import ErrorHandler
from models.execution import Batch
from uploaders import utils
from uploaders.uploaders import MegalistaUploader

_LOGGER_NAME: str = 'megalista.CampaignManagerConversionsUploader'


class CampaignManagerConversionUploaderDoFn(MegalistaUploader):

  def __init__(self, oauth_credentials, error_handler: ErrorHandler):
    super().__init__(error_handler)
    self.oauth_credentials = oauth_credentials

  def _get_dcm_service(self):
    credentials = Credentials(
        token=self.oauth_credentials.get_access_token(),
        refresh_token=self.oauth_credentials.get_refresh_token(),
        client_id=self.oauth_credentials.get_client_id(),
        client_secret=self.oauth_credentials.get_client_secret(),
        token_uri='https://accounts.google.com/o/oauth2/token',
        scopes=[
            'https://www.googleapis.com/auth/dfareporting',
            'https://www.googleapis.com/auth/dfatrafficking',
            'https://www.googleapis.com/auth/ddmconversions'])

    return build('dfareporting', 'v4', credentials=credentials)

  def start_bundle(self):
    pass

  @staticmethod
  def _assert_all_list_names_are_present(any_execution):
    destination = any_execution.destination.destination_metadata
    if len(destination) != 2:
      raise ValueError(
          f'Missing destination information. Found {len(destination)}')

    if not destination[0] \
        or not destination[1]:
      raise ValueError(
          f'Missing destination information. Received {str(destination)}')

  @utils.safe_process(logger=logging.getLogger(_LOGGER_NAME))
  def process(self, batch: Batch, **kwargs):
    self._do_process(batch, time.time())
    return [batch]

  def _do_process(self, batch: Batch, timestamp):
    execution = batch.execution
    self._assert_all_list_names_are_present(execution)

    self._do_upload_data(
        execution,
        execution.destination.destination_metadata[0],
        execution.destination.destination_metadata[1],
        execution.account_config.campaign_manager_profile_id,
        timestamp,
        batch.elements)

  def _do_upload_data(
      self,
      execution,
      floodlight_activity_id,
      floodlight_configuration_id,
      campaign_manager_profile_id,
      timestamp,
      rows):

    service = self._get_dcm_service()
    conversions = []
    logger = logging.getLogger(_LOGGER_NAME)
    for conversion in rows:
      to_upload = {
          'floodlightActivityId': floodlight_activity_id,
          'floodlightConfigurationId': floodlight_configuration_id,
          'ordinal': math.floor(timestamp * 10e5),
          'timestampMicros': math.floor(timestamp * 10e5)
      }

      if 'gclid' in conversion and conversion['gclid']:
        to_upload['gclid'] = conversion['gclid']
      elif 'encryptedUserId' in conversion and conversion['encryptedUserId']:
        to_upload['encryptedUserId'] = conversion['encryptedUserId']
      elif 'mobileDeviceId' in conversion and conversion['mobileDeviceId']:
        to_upload['mobileDeviceId'] = conversion['mobileDeviceId']
      elif 'matchId' in conversion and conversion['matchId']:
        to_upload['matchId'] = conversion['matchId']
      elif 'dclid' in conversion and conversion['dclid']:
        to_upload['dclid'] = conversion['dclid']

      if 'value' in conversion:
        to_upload['value'] = float(conversion['value'])
      if 'quantity' in conversion:
        to_upload['quantity'] = conversion['quantity']
      if 'customVariables' in conversion:
        custom_variables = []
        for r in conversion['customVariables']:
          cv = {
              'type': r['type'],
              'value': r['value'],
              'kind': 'dfareporting#customFloodlightVariable',
          }
          custom_variables.append(cv)
        to_upload['customVariables'] = custom_variables

      if 'timestamp' in conversion:
        to_upload['timestampMicros'] = utils.get_timestamp_micros(conversion['timestamp'])

      conversions.append(to_upload)

    request_body = {
      'conversions': conversions,
    }

    logger.info(f'Conversions: \n{conversions}', execution=execution)

    request = service.conversions().batchinsert(
        profileId=campaign_manager_profile_id, body=request_body)
    response = request.execute()

    if response['hasFailures']:
      logger.error(f'Error(s) inserting conversions:\n{response}', execution=execution)
      conversions_status = response['status']
      error_messages = []

      for status in conversions_status:
        if 'errors' in status:
          for error in status['errors']:
            error_messages.append('[{}]: {}'.format(error['code'], error['message']))

      final_error_message = 'Errors from API:\n{}'.format('\n'.join(error_messages))
      logger.error(final_error_message, execution=execution)
      self._add_error(execution, final_error_message)
