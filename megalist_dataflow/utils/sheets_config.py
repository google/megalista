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

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from utils.oauth_credentials import OAuthCredentials
from apache_beam.options.value_provider import StaticValueProvider


class SheetsConfig:
    def __init__(self, oauth_credentials):
        credentials = Credentials(
            token=oauth_credentials.get_access_token(),
            refresh_token=oauth_credentials.get_refresh_token(),
            client_id=oauth_credentials.get_client_id(),
            client_secret=oauth_credentials.get_client_secret(),
            token_uri='https://accounts.google.com/o/oauth2/token',
            scopes=['https://www.googleapis.com/auth/spreadsheets.readonly'])

        self.sheets_service = build('sheets', 'v4', credentials=credentials)

    def to_dict(self, config):
        return dict(map(lambda x: (x[0], {"op": x[1], "value": x[2], "multiplier": x[3]}), config))

    def get_config(self, sheet_id, range):
        config_range = self.sheets_service.spreadsheets().values().get(
            spreadsheetId=sheet_id, range=range).execute()
        return self.to_dict(config_range['values'])
