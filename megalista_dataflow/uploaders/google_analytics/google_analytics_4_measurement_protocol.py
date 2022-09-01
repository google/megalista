# Copyright 2021 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import json
from config import logging
from typing import Dict, Any, Tuple, Optional

import requests

from error.error_handling import ErrorHandler
from models.execution import Batch, Execution
from uploaders import utils
from uploaders.uploaders import MegalistaUploader

LOGGER_NAME = 'megalista.GoogleAnalytics4MeasurementProtocolUploader'

class GoogleAnalytics4MeasurementProtocolUploaderDoFn(MegalistaUploader):
  def __init__(self, error_handler: ErrorHandler):
    super().__init__(error_handler)
    self.API_URL = 'https://www.google-analytics.com/mp/collect'

  @staticmethod
  def _str2bool(s: str) -> bool:
    return s.lower() == 'true'

  @staticmethod
  def _exactly_one_of(a: Any, b: Any) -> bool:
    return (a and not b) or (not a and b)

  @utils.safe_process(logger=logging.get_logger(LOGGER_NAME))
  def process(self, batch: Batch, **kwargs):
    return self.do_process(batch)

  # Created to facilitate testing without going into @utils.safe_process
  def do_process(self, batch: Batch):
    execution = batch.execution

    api_secret = execution.destination.destination_metadata[0]
    is_event = self._str2bool(execution.destination.destination_metadata[1])
    is_user_property = self._str2bool(execution.destination.destination_metadata[2])
    non_personalized_ads = self._str2bool(execution.destination.destination_metadata[3])      
    
    firebase_app_id, measurement_id = self._getIds(execution, is_event, is_user_property)

    accepted_elements = []

    for row in batch.elements:
      app_instance_id = row.get('app_instance_id')
      client_id = row.get('client_id')
      user_id = row.get('user_id')
      
      if not self._exactly_one_of(app_instance_id, client_id):
        raise ValueError(
          'GA4 MP should be called either with an app_instance_id (for apps) or a client_id (for web)')

      if firebase_app_id and not app_instance_id:
        raise ValueError(
          'GA4 MP needs an app_instance_id parameter when used for an App Stream.')

      if measurement_id and not client_id:
        raise ValueError(
          'GA4 MP needs a client_id parameter when used for a Web Stream.')

      
      url = self._getUrl(api_secret, firebase_app_id, measurement_id)

      payload = self._fillPayload(row, non_personalized_ads, is_event, is_user_property, firebase_app_id, app_instance_id, measurement_id, client_id, user_id)

      response = requests.post(url,data=json.dumps(payload))
      if response.status_code != 204:
        error_message = f'Error calling GA4 MP {response.status_code}: {str(response.content)}'
        logging.get_logger(LOGGER_NAME).error(error_message, execution=execution)
        self._add_error(execution, error_message)
      else:
        accepted_elements.append(row)

    logging.get_logger(LOGGER_NAME).info(
      f'Successfully uploaded {len(accepted_elements)}/{len(batch.elements)} events.', execution=execution)

    execution.successful_records = execution.successful_records + len(accepted_elements)
    execution.unsuccessful_records = execution.unsuccessful_records + (len(batch.elements) - len(accepted_elements))
    
    return [Batch(execution, accepted_elements)]

  def _getIds(self, execution: Execution, is_event: bool, is_user_property: bool) -> Tuple[Optional[str], Optional[str]]:
    firebase_app_id = None
    if len(execution.destination.destination_metadata) >= 5:
      firebase_app_id = execution.destination.destination_metadata[4]

    measurement_id = None
    if len(execution.destination.destination_metadata) >= 6:
      measurement_id = execution.destination.destination_metadata[5]
     
    if not self._exactly_one_of(firebase_app_id, measurement_id):
          raise ValueError(
            'GA4 MP should be called either with a firebase_app_id (for apps) or a measurement_id (for web)')      

    if not self._exactly_one_of(is_event, is_user_property):
          raise ValueError(
            'GA4 MP should be called either for sending events or a user properties')  
    
    return firebase_app_id, measurement_id

  def _getUrl(self, api_secret, firebase_app_id, measurement_id):
    url_container = [f'{self.API_URL}?api_secret={api_secret}']

    if firebase_app_id:
      url_container.append(f'&firebase_app_id={firebase_app_id}')
      
    if measurement_id:
      url_container.append(f'&measurement_id={measurement_id}')

    return ''.join(url_container)

  def _fillPayload(self, row, non_personalized_ads, is_event, is_user_property, firebase_app_id, app_instance_id, measurement_id, client_id, user_id):
    payload: Dict[str, Any] = {
      'nonPersonalizedAds': non_personalized_ads
    }

    if 'timestamp_micros' in row:
        payload['timestamp_micros'] = int(str(row.get('timestamp_micros')))
    
    if is_event:
      params = {k: v for k, v in row.items() if k not in ('name', 'app_instance_id', 'client_id', 'uuid', 'user_id', 'timestamp_micros') and v is not None}
      payload['events'] = [{'name': row['name'], 'params': params}]

    if is_user_property: 
      payload['userProperties'] = {k: {'value': v} for k, v in row.items() if k not in ('app_instance_id', 'client_id', 'uuid', 'user_id', 'timestamp_micros') and v is not None}
      payload['events'] = {'name': 'user_property_addition_event', 'params': {}}

    if firebase_app_id:
      payload['app_instance_id'] = app_instance_id
      
    if measurement_id:
      payload['client_id'] = client_id

    if user_id:
      payload['user_id'] = user_id
  
    return payload
