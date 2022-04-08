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
import re
from typing import Dict, Any
from urllib.parse import quote

import requests

from error.error_handling import ErrorHandler
from models.execution import Batch
from uploaders import utils
from uploaders.uploaders import MegalistaUploader


class GoogleAnalyticsMeasurementProtocolUploaderDoFn(MegalistaUploader):
  def __init__(self, error_handler: ErrorHandler):
    super().__init__(error_handler)
    self.API_URL = "https://www.google-analytics.com/batch"
    self.UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36"

  def start_bundle(self):
    pass

  def _format_hit(self, payload: Dict[str, Any]) -> str:
    return "&".join([key + "=" + quote(str(value)) for key, value in payload.items() if value is not None])

  @utils.safe_process(logger=logging.getLogger("megalista.GoogleAnalyticsMeasurementProtocolUploader"))
  def process(self, batch: Batch, **kwargs):
    execution = batch.execution
    rows = batch.elements
    payloads = [{
      "v": 1,
      "tid": execution.destination.destination_metadata[0],
      "ni": execution.destination.destination_metadata[1],
      "t": "event",
      "ds": "mp - megalista",
      **{'cid': row[key] for key in row.keys() if key.startswith("client_id")},
      **{'uid': row[key] for key in row.keys() if key.startswith("user_id")},
      "ea": row['event_action'],
      "ec": row['event_category'],
      "ev": row.get('event_value'),
      "el": row.get('event_label'),
      "ua": self.UA,
      **{key: row[key] for key in row.keys() if re.match('c[dm]\d+', key)}
    } for row in rows]

    encoded = [self._format_hit(payload) for payload in payloads]

    payload = '\n'.join(encoded)
    response = requests.post(url=self.API_URL, data=payload)
    if response.status_code != 200:
      error_message = f"Error uploading to Analytics HTTP {response.status_code}: {response.raw}"
      logging.getLogger("megalista.GoogleAnalyticsMeasurementProtocolUploader").error(error_message)
      self._add_error(execution, error_message)
    else:
      return [batch]
