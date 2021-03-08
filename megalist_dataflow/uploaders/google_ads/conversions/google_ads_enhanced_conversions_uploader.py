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


import logging
from typing import Dict, Any, Union, List
from urllib.parse import quote

import apache_beam as beam
import requests
import json

from uploaders import utils
from models.execution import DestinationType, Batch
from models.oauth_credentials import OAuthCredentials


class GoogleAdsEnhancedConversionsUploaderDoFn(beam.DoFn):
    def __init__(self, oauth_credentials: OAuthCredentials):
        super().__init__()
        self._api_url = "https://www.google.com/ads/event/api/v1"
        self._ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36"
        self._oauth_credentials = oauth_credentials

    def _format_query_params(self, payload: Dict[str, Any]) -> str:
        return "&".join([key + "=" + quote(str(value)) for key, value in payload.items() if value is not None])

    def _get_access_token(self):
        payload = {
            'client_id': self._oauth_credentials.get_client_id(),
            'client_secret': self._oauth_credentials.get_client_secret(),
            'refresh_token': self._oauth_credentials.get_refresh_token(),
            'grant_type': 'refresh_token'
        }
        response = requests.post(
            url='https://oauth2.googleapis.com/token', data=payload)
        return response.json()['access_token']

    @utils.safe_process(logger=logging.getLogger("megalista.GoogleAdsEnhancedConversionsUploaderDoFn"))
    def process(self, batch: Batch, **kwargs):
        execution = batch.execution
        rows = batch.elements
        headers = {
            "Authorization": f"Bearer {self._get_access_token()}"
        }

        for row in rows:
            payload: Dict[str, Any] = {
                'pii_data': {},
                'user_agent': self._ua
            }
            if 'addressInfo' in row and isinstance(row['addressInfo'], dict):
                address_info: Dict[str, str] = row['addressInfo']
                payload['pii_data']['address'] = [{
                    'hashed_first_name': address_info['hashedFirstName'],
                    'hashed_last_name': address_info['hashedLastName'],
                    'country': address_info['countryCode'],
                    'postcode': address_info['zipCode']
                }]
            if 'hashedEmail' in row:
                payload['pii_data']['hashed_email'] = row['hashedEmail']
            if 'hashedPhoneNumber' in row:
                payload['pii_data']['hashed_phone_number'] = row['hashedPhoneNumber']

            query_params = {
                'gclid': row['gclid'],
                'conversion_time': row['conversion_time'],
                'conversion_tracking_id': execution.destination.destination_metadata[1],
                'label': execution.destination.destination_metadata[0],
                'oid': row['oid'],
                'value': row['value'],
                'currency_code': execution.destination.destination_metadata[2]
            }

            url = f"{self._api_url}?{self._format_query_params(query_params)}"

            response = requests.post(url=url, json=payload, headers=headers)
            if response.status_code != 200:
                logging.getLogger('megalista.GoogleAdsEnhancedConversionsUploaderDoFn').error(
                    f"Error uploading Enhanced Conversion {response.status_code}: {response.json()}")

        yield batch
