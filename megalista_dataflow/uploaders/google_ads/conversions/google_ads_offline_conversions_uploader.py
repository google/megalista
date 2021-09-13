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

import apache_beam as beam
from models.execution import Batch, Execution, AccountConfig, Destination
from uploaders import utils
from uploaders.google_ads import ADS_API_VERSION

_DEFAULT_LOGGER: str = 'megalista.GoogleAdsOfflineConversionsUploader'


class GoogleAdsOfflineUploaderDoFn(beam.DoFn):

  def __init__(self, oauth_credentials, developer_token):
    super().__init__()
    self.oauth_credentials = oauth_credentials
    self.developer_token = developer_token

  def _get_ads_service(self, customer_id: str):
    return utils.get_ads_service('GoogleAdsService', ADS_API_VERSION,
                                     self.oauth_credentials,
                                     self.developer_token.get(), 
                                     customer_id)
  
  def _get_oc_service(self, customer_id):
    return utils.get_ads_service('ConversionUploadService', ADS_API_VERSION,
                                     self.oauth_credentials,
                                     self.developer_token.get(), 
                                     customer_id)

  def start_bundle(self):
    pass

  def _get_customer_id(self, account_config:AccountConfig, destination:Destination) -> str:
    """
      If the customer_id is present on the destination, returns it, otherwise defaults to the account_config info.
    """
    if len(destination.destination_metadata) >= 2 and len(destination.destination_metadata[1]) > 0:
      return destination.destination_metadata[1].replace('-', '')
    return account_config.google_ads_account_id.replace('-', '')


  @staticmethod
  def _assert_conversion_name_is_present(execution: Execution):
    destination = execution.destination.destination_metadata
    if len(destination) is 0:
      raise ValueError('Missing destination information. Found {}'.format(
          len(destination)))

    if not destination[0]:
      raise ValueError('Missing destination information. Received {}'.format(
          str(destination)))

  @utils.safe_process(
      logger=logging.getLogger('megalista.GoogleAdsOfflineUploader'))
  def process(self, batch: Batch, **kwargs):
    execution = batch.execution
    self._assert_conversion_name_is_present(execution)

    customer_id = self._get_customer_id(execution.account_config, execution.destination)
    oc_service = self._get_oc_service(customer_id)
    
    resource_name = self._get_resource_name(customer_id, execution.destination.destination_metadata[0])

    self._do_upload(oc_service,
                    resource_name,
                    customer_id,
                    batch.elements)

  @staticmethod
  def _do_upload(oc_service, conversion_resource_name, customer_id, rows):
    logging.getLogger(_DEFAULT_LOGGER).info(f'Uploading {len(rows)} offline conversions on {conversion_resource_name} to Google Ads.')
    conversions = [{
          'conversion_action': conversion_resource_name,
          'conversion_date_time': utils.format_date(conversion['time']),
          'conversion_value': int(conversion['amount']),
          'gclid': conversion['gclid']
    } for conversion in rows]
    
    upload_data = {
      'customer_id': customer_id,
      'partial_failure': True,
      'validate_only': False,
      'conversions': conversions
    }

    response = oc_service.upload_click_conversions(request=upload_data)
    utils.print_partial_error_messages(_DEFAULT_LOGGER, 'uploading offline conversions', response)

  def _get_resource_name(self, customer_id: str, name: str):
      service = self._get_ads_service(customer_id)
      query = f"SELECT conversion_action.resource_name FROM conversion_action WHERE conversion_action.name = '{name}'"
      response_query = service.search_stream(customer_id=customer_id, query=query)
      for batch in response_query:
        for row in batch.results:
          return row.conversion_action.resource_name
      raise Exception(f'Conversion "{name}" could not be found on account {customer_id}')
