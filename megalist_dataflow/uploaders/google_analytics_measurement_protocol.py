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
from urllib.parse import quote
import requests
import json
import apache_beam as beam
from typing import Dict, Any
from googleapiclient.discovery import build
from googleapiclient.http import MediaInMemoryUpload
from google.oauth2.credentials import Credentials

from uploaders import google_ads_utils as ads_utils
from uploaders import utils as utils
from utils.execution import DestinationType


class GoogleAnalyticsMeasurementProtocolUploaderDoFn(beam.DoFn):
    def __init__(self, oauth_credentials: Credentials):
        super().__init__()
        self.API_URL = "https://www.google-analytics.com/batch"
        self.UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36"

    def start_bundle(self):
        pass

    def _format_hit(self, payload: Dict[str, Any]) -> str:
        return "&".join([key + "=" + quote(str(value)) for key, value in payload.items()])

    @utils.safe_process(logger=logging.getLogger("megalista.GoogleAnalyticsMeasurementProtocolUploader"))
    def process(self, elements, **kwargs):
        any_execution = elements[0]['execution']
        rows = utils.extract_rows(elements)
        payloads = [{
            "v": 1,
            "tid": any_execution.destination.destination_metadata[0],
            "ni": any_execution.destination.destination_metadata[1],
            "t": "event",
            "ds": "mp - megalista",
            "cid": row['client_id'],
            "ea": row['event_action'],
            "ec": row['event_category'],
            "ev": row['event_value'],
            "el": row['event_label'],
            "ua": self.UA,
            **{key: row[key] for key in list(filter(lambda key: key.startswith("cd"), row.keys()))}
        } for row in rows]
        encoded = [self._format_hit(payload) for payload in payloads]
        payload = '\n'.join(encoded)
        response = requests.post(url=self.API_URL, data=payload)
        if (response.status_code != 200):
            raise Exception(
                f"Error uploading to Analytics HTTP {response.status_code}: {response.raw}")
