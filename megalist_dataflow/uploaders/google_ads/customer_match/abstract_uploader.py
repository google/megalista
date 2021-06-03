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
from typing import Dict, Any, List

import apache_beam as beam
from uploaders import utils
from models.execution import AccountConfig, Destination
from models.execution import DestinationType
from models.execution import Batch
from models.oauth_credentials import OAuthCredentials

_DEFAULT_LOGGER: str = 'megalista.GoogleAdsCustomerMatchAbstractUploader'


class GoogleAdsCustomerMatchAbstractUploaderDoFn(beam.DoFn):

  def __init__(self, oauth_credentials: OAuthCredentials, developer_token: str):
    super().__init__()
    self.oauth_credentials = oauth_credentials
    self.developer_token = developer_token
    self.active = True
    if self.developer_token is None:
      self.active = False
    self._user_list_id_cache: Dict[str, str] = {}

  def start_bundle(self):
    pass

  def _create_list_if_it_does_not_exist(self, 
                                        customer_id: str,
                                        list_name: str,
                                        list_definition: Dict[str, Any]) -> str:

    if self._user_list_id_cache.get(list_name) is None:
      self._user_list_id_cache[list_name] = \
        self._do_create_list_if_it_does_not_exist(
            customer_id, list_name, list_definition)

    return self._user_list_id_cache[list_name]

  def _do_create_list_if_it_does_not_exist(self,
                                           customer_id: str,
                                           list_name: str,
                                           list_definition: Dict[str, Any]
                                           ) -> str:
    
    resource_name = self._get_user_list_resource_name(customer_id, list_name)
    
    if resource_name is None:
      # Create list
      logging.getLogger(_DEFAULT_LOGGER).info(
        '%s list does not exist, creating...', list_name)
      request = {
        'customer_id': customer_id,
        'partial_failure': False,
        'validate_only': False,
        'operations': [{
          'create': list_definition
        }]
      }
      
      user_list_service = self._get_user_list_service(customer_id)
      user_list_service_response = user_list_service.mutate_user_lists(request)
      for result in user_list_service_response.results:
        resource_name = result.resource_name
      logging.getLogger(_DEFAULT_LOGGER).info('List %s created with resource name: %s',
                                              list_name, resource_name)
    else:
      logging.getLogger(_DEFAULT_LOGGER).info('List %s found with resource name: %s',
                                              list_name, resource_name)
    return resource_name

  def _get_user_list_resource_name(self, customer_id: str, list_name: str):
    ads_client = utils.get_ads_client(self.oauth_credentials, self.developer_token.get(), customer_id)

    resource_name = None
    service = self._get_ads_service(customer_id)

    # Only search for audiences owned by this account, not MCCs above it.
    query = f"SELECT user_list.resource_name, user_list.access_reason FROM user_list WHERE user_list.name='{list_name}' " \
            f"AND user_list.access_reason={ads_client.enums.AccessReasonEnum.OWNED.name}"
    response_query = service.search_stream(customer_id=customer_id, query=query)
    for batch in response_query:
      for row in batch.results:
        resource_name = row.user_list.resource_name
    return resource_name

  # just to facilitate mocking
  def _get_ads_service(self, customer_id: str):
    return utils.get_ads_service('GoogleAdsService', 'v7',
                                     self.oauth_credentials,
                                     self.developer_token.get(), 
                                     customer_id)
  
  def _get_user_list_service(self, customer_id: str):
    return utils.get_ads_service('UserListService', 'v7',
                                     self.oauth_credentials,
                                     self.developer_token.get(), 
                                     customer_id)

  def _get_offline_user_data_job_service(self, customer_id: str):
    return utils.get_ads_service('OfflineUserDataJobService', 'v7',
                                    self.oauth_credentials,
                                    self.developer_token.get(), 
                                    customer_id)

  def _assert_execution_is_valid(self, execution) -> None:
    destination = execution.destination.destination_metadata

    # The number of parameters vary by upload. This test could be parameterized
    if not destination[0]:
      raise ValueError('Missing destination information. Received {}'.format(
          str(destination)))

  def _get_customer_id(self, account_config:AccountConfig, destination:Destination) -> str:
    """
      If the customer_id is present on the destination, returns it, otherwise defaults to the account_config info.
    """
    if len(destination.destination_metadata) >= 5 and len(destination.destination_metadata[4]) > 0:
      return destination.destination_metadata[4].replace('-', '')
    return account_config.google_ads_account_id.replace('-', '')

  @utils.safe_process(logger=logging.getLogger(_DEFAULT_LOGGER))
  def process(self, batch: Batch, **kwargs) -> None:
    if not self.active:
      logging.getLogger(_DEFAULT_LOGGER).warning(
          'Skipping upload to ads, parameters not configured.')
      return

    execution = batch.execution

    self._assert_execution_is_valid(execution)

    customer_id = self._get_customer_id(execution.account_config, execution.destination)

    # get API services
    offline_user_data_job_service = self._get_offline_user_data_job_service(
      customer_id)

    list_resource_name = self._create_list_if_it_does_not_exist(
      customer_id, execution.destination.destination_metadata[0],
      self.get_list_definition(
        execution.account_config,
        execution.destination.destination_metadata))

    rows = self.get_filtered_rows(
      batch.elements, self.get_row_keys())

    # Upload is divided into 3 parts:
    # 1. Create Job
    # 2. Create operations (data insertion)
    # 3. Run the Job

    # 1. Create Job
    job_creation_payload = {
      'type_': 'CUSTOMER_MATCH_USER_LIST',
      'customer_match_user_list_metadata': {
        'user_list': list_resource_name
      }
    }

    job_resource_name = offline_user_data_job_service.create_offline_user_data_job(customer_id = customer_id, job = job_creation_payload).resource_name

    # 2. Crete operations (data insertion)
    operator = self._get_list_operator(execution.destination.destination_metadata[1])
    data_insertion_payload = {
      'resource_name': job_resource_name,
      'enable_partial_failure': False,
      'operations': [{
        operator: {
          'user_identifiers': rows
        }
      }]
    }

    data_insertion_response = offline_user_data_job_service.add_offline_user_data_job_operations(request = data_insertion_payload)

    utils.print_partial_error_messages(_DEFAULT_LOGGER, 'uploading customer match', data_insertion_response)
    
    # 3. Run the Job
    offline_user_data_job_service.run_offline_user_data_job(resource_name = job_resource_name)

  def _get_list_operator(self, operator: str) -> str:
    translation = {
      'ADD': 'create',
      'REMOVE': 'remove'
    }
    return translation[operator]

  def get_filtered_rows(self, rows: List[Any],
                        keys: List[str]) -> List[Dict[str, Any]]:
    return [{key: row.get(key) for key in keys} for row in rows]

  def get_list_definition(self, account_config: AccountConfig,
                          destination_metadata: List[str]) -> Dict[str, Any]:
    pass

  def get_row_keys(self) -> List[str]:
    pass

  def get_action_type(self) -> DestinationType:
    pass
