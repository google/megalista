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

import time
import math
import logging
from google.oauth2.credentials import Credentials
from apiclient.discovery import build
import apache_beam as beam


class CampaignManagerConversionUploaderDoFn(beam.DoFn):
    def __init__(self, oauth_credentials, dcm_profile_id, floodlight_activity_id, floodlight_configuration_id):
        self.dcm_profile_id = dcm_profile_id
        self.floodlight_activity_id = floodlight_activity_id
        self.floodlight_configuration_id = floodlight_configuration_id
        self.oauth_credentials = oauth_credentials

    def _disabled(self):
        return self.dcm_profile_id.get() is None or self.floodlight_activity_id.get() is None or self.floodlight_configuration_id.get() is None

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

        service = build('dfareporting', 'v3.2',
                        credentials=credentials)
        return service

    def start_bundle(self):
        if self._disabled():
            logging.getLogger().warn(
                "Skipping upload to Campaign Manager, parameters not configured.")

    def process(self, elements):
        if self._disabled():
            return
        service = self._get_dcm_service()
        conversions = [{
            'gclid': conversion['gclid'],
            'floodlightActivityId': self.floodlight_activity_id.get(),
            'floodlightConfigurationId': self.floodlight_configuration_id.get(),
            'ordinal': math.floor(time.time()*10e5),
            'timestampMicros': math.floor(time.time()*10e5)
        } for conversion in elements]

        request_body = {
            'conversions': conversions,
            'encryptionInfo': 'AD_SERVING'
        }
        request = service.conversions().batchinsert(profileId=self.dcm_profile_id.get(),
                                                    body=request_body)
        response = request.execute()
        if response['hasFailures']:
            logging.getLogger().error('Error(s) inserting conversions:')
            status = response['status'][0]
            for error in status['errors']:
                logging.getLogger().error('\t[%s]: %s' % (
                    error['code'], error['message']))
