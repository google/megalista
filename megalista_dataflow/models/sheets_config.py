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

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build


class SheetsConfig:
  def __init__(self, oauth_credentials):
    self._oauth_credentials = oauth_credentials
    self._sheets_service = None

  def _get_sheets_service(self):
    if not self._sheets_service:
      credentials = Credentials(
        token=self._oauth_credentials.get_access_token(),
        refresh_token=self._oauth_credentials.get_refresh_token(),
        client_id=self._oauth_credentials.get_client_id(),
        client_secret=self._oauth_credentials.get_client_secret(),
        token_uri='https://accounts.google.com/o/oauth2/token',
        scopes=['https://www.googleapis.com/auth/spreadsheets.readonly'])

      self._sheets_service = build('sheets', 'v4', credentials=credentials)
    return self._sheets_service

  def to_dict(self, config):
    return dict(map(lambda x: (x[0], {"op": x[1], "value": x[2], "multiplier": x[3]}), config))

  def get_config(self, sheet_id, range):
    config_range = self.get_range(sheet_id, range)
    return self.to_dict(config_range['values'])

  def get_range(self, sheet_id, range):
    return self._get_sheets_service().spreadsheets().values().get(spreadsheetId=sheet_id, range=range).execute()

  def get_value(self, sheet_id, range):
    range = self.get_range(sheet_id, range)
    if range.get('values') is None:
      return None
    return range['values'][0][0]

  def check_if_range_exists(self, sheet_id, range):
      try:
        self._get_sheets_service().spreadsheets().get(spreadsheetId=sheet_id, range=range).execute()
      except:
        return False
      else:
        return True
      finally:
        pass 
