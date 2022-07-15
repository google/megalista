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

  def build_hit(self, batch: Batch, row: Dict[str, Any]) -> Dict[str, Any]:
    metadata_list = batch.execution.destination.destination_metadata

    # initialize payload with values that are common to all types of hit
    payload = {
      "v": 1,
      "tid": metadata_list[0],
      "ni": metadata_list[1],
      "ds": "mp - megalista",
      **{'cid': row[key] for key in row.keys() if key.startswith("client_id")},
      **{'uid': row[key] for key in row.keys() if key.startswith("user_id")},
      "ua": self.UA,
      **{key: row[key] for key in row.keys() if re.match('c[dm]\d+', key)},
      **{'cs': row[key] for key in row.keys() if key.startswith("campaign_source")},
      **{'cm': row[key] for key in row.keys() if key.startswith("campaign_medium")}
    }

    # get hit type from metadata or default to "event" if not provided
    hit_type = metadata_list[2] if len(metadata_list) > 2 else "event"

    if hit_type == "event":
      payload["t"] = hit_type
      payload["ea"] = row['event_action']
      payload["ec"] = row['event_category']
      payload["ev"] = row.get('event_value')
      payload["el"] = row.get('event_label')
    elif hit_type = "transaction":
      payload["t"] = hit_type
      payload["ti"] = row['transaction_id']   # Transaction ID. Required.
      payload["ta"] = row.get('transaction_affiliation')   # Transaction affiliation.
      payload["tr"] = row.get('transaction_revenue')   # Transaction revenue.
      payload["ts"] = row.get('transaction_shipping')   # Transaction shipping.
      payload["tt"] = row.get('transaction_tax')   # Transaction tax.
      payload["cu"] = row.get('currency_code')   # Currency code.      
    elif hit_type = "item":
      payload["t"] = hit_type
      payload["ti"] = row['transaction_id']   # Transaction ID. Required.
      payload["in"] = row.get('item_name')  # Item name. Required.
      payload["ip"] = row.get('item_price')  # Item price.
      payload["iq"] = row.get('item_quantity')  # Item quantity.
      payload["ic"] = row.get('item_code')  # Item code / SKU.
      payload["iv"] = row.get('item_variation')  # Item variation / category.
      payload["cu"] = row.get('currency_code')  # Currency code.      
    else:
      error_message = f"Hit type {hit_type} is not supported."
      logging.getLogger("megalista.GoogleAnalyticsMeasurementProtocolUploader").error(error_message)
      self._add_error(batch.execution, error_message)

    return payload

  @utils.safe_process(logger=logging.getLogger("megalista.GoogleAnalyticsMeasurementProtocolUploader"))
  def process(self, batch: Batch, **kwargs):
    rows = batch.elements

    # parameters starting with ** are optional.
    payloads = [self.build_hit(batch, row) for row in rows]

    encoded = [self._format_hit(payload) for payload in payloads]

    payload = '\n'.join(encoded)
    response = requests.post(url=self.API_URL, data=payload)
    if response.status_code != 200:
      error_message = f"Error uploading to Analytics HTTP {response.status_code}: {response.raw}"
      logging.getLogger("megalista.GoogleAnalyticsMeasurementProtocolUploader").error(error_message)
      self._add_error(batch.execution, error_message)
    else:
      return [batch]
