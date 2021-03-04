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
from typing import Dict, Any
from urllib.parse import quote

import apache_beam as beam
import requests
import json

from uploaders import utils
from models.execution import DestinationType, Batch


class GoogleAnalytics4MeasurementProtocolUploaderDoFn(beam.DoFn):
  def __init__(self):
    super().__init__()
    self.API_URL = "https://www.google-analytics.com/mp/collect"

  def start_bundle(self):
    pass

  @staticmethod
  def _str2bool(s: str) -> bool:
    return s.lower() == 'true'

  @staticmethod
  def _exactly_one_of(a: Any, b: Any) -> bool:
    return (a and not b) or (not a and b)

  @utils.safe_process(logger=logging.getLogger("megalista.GoogleAnalytics4MeasurementProtocolUploader"))
  def process(self, batch: Batch, **kwargs):
    execution = batch.execution

    api_secret = execution.destination.destination_metadata[0]
    is_event = self._str2bool(execution.destination.destination_metadata[1])
    is_user_property = self._str2bool(execution.destination.destination_metadata[2])
    non_personalized_ads = self._str2bool(execution.destination.destination_metadata[3])

    firebase_app_id = None
    if len(execution.destination.destination_metadata) >= 5:
      firebase_app_id = execution.destination.destination_metadata[4]

    measurement_id = None
    if len(execution.destination.destination_metadata) >= 6:
      measurement_id = execution.destination.destination_metadata[5]
     
    if not self._exactly_one_of(firebase_app_id, measurement_id):
          raise ValueError(
            f"GA4 MP should be called either with a firebase_app_id (for apps) or a measurement_id (for web)")      

    if not self._exactly_one_of(is_event, is_user_property):
          raise ValueError(
            f"GA4 MP should be called either for sending events or a user properties")        
    
    payload = {
      "nonPersonalizedAds": non_personalized_ads
    }

    accepted_elements = []

    for row in batch.elements:
      app_instance_id = row.get('app_instance_id')
      client_id = row.get('client_id')

      if not self._exactly_one_of(app_instance_id, client_id):
        logging.getLogger("megalista.GoogleAnalytics4MeasurementProtocolUploader").error(
          f"GA4 MP should be called either with an app_instance_id (for apps) or a client_id (for web)")
    
      if is_event:
        params = {k: v for k, v in row.items() if k not in ("name", "app_instance_id", "client_id", "uuid")}
        payload["events"] = [{"name": row["name"], "params": params}]

      if is_user_property: 
        payload["userProperties"] = {k: {"value": v} for k, v in row.items() if k not in ("app_instance_id", "client_id", "uuid")}
        payload["events"] = {"name": "user_property_addition_event", "params": {}}

      url_container = [f"{self.API_URL}?api_secret={api_secret}"]

      if firebase_app_id:
        url_container.append(f"&firebase_app_id={firebase_app_id}")
        payload["app_instance_id"] = app_instance_id

      if measurement_id:
        url_container.append(f"&measurement_id={measurement_id}")
        payload["client_id"] = client_id

      url = ''.join(url_container)
      response = requests.post(url,data=json.dumps(payload))
      if response.status_code != 204:
        logging.getLogger("megalista.GoogleAnalytics4MeasurementProtocolUploader").error(
          f"Error calling GA4 MP {response.status_code}: {response.raw}")
      else:
        accepted_elements.append(row)

    logging.getLogger("megalista.GoogleAnalytics4MeasurementProtocolUploader").info(
      f"Successfully uploaded {len(accepted_elements)}/{len(batch.elements)} events.")
    yield Batch(execution, accepted_elements)
