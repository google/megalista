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
from typing import Dict, Any, List

import apache_beam as beam
from uploaders import google_ads_utils as ads_utils
from uploaders import utils
from utils.execution import AccountConfig
from utils.execution import DestinationType
from utils.oauth_credentials import OAuthCredentials

_DEFAULT_LOGGER: str = 'megalista.GoogleAdsCustomerMatchAbstractUploader'


class GoogleAdsCustomerMatchAbstractUploaderDoFn(beam.DoFn):

  def __init__(self, oauth_credentials: OAuthCredentials, developer_token: str):
    super().__init__()
    self.oauth_credentials = oauth_credentials
    self.developer_token = developer_token
    self.active = True
    if self.developer_token is None:
      self.active = False
    self._user_list_id_cache = {}

  def start_bundle(self):
    pass

  def _create_list_if_it_does_not_exist(self, user_list_service, list_name: str,
                                        list_definition: Dict[str, Any]) -> str:

    if self._user_list_id_cache.get(list_name) is None:
      self._user_list_id_cache[list_name] = \
        self._do_create_list_if_it_does_not_exist(
            user_list_service, list_name, list_definition)

    return self._user_list_id_cache[list_name]

  def _do_create_list_if_it_does_not_exist(self, user_list_service,
                                           list_name: str,
                                           list_definition: Dict[str, Any]
                                           ) -> str:
    response = user_list_service.get([{
        'fields': ['Id', 'Name'],
        'predicates': [{
            'field': 'Name',
            'operator': 'EQUALS',
            'values': [list_name]
        }]
    }])

    if not response.entries:
      logging.getLogger(_DEFAULT_LOGGER).info(
          '%s list does not exist, creating...', list_name)
      result = user_list_service.mutate([{
          'operator': 'ADD',
          **list_definition
      }])
      list_id = result['value'][0]['id']
      logging.getLogger(_DEFAULT_LOGGER).info('List %s created with id: %d',
                                              list_name, list_id)
    else:
      list_id = response.entries[0]['id']
      logging.getLogger(_DEFAULT_LOGGER).info('List found %s with id: %d',
                                              list_name, list_id)

    return list_id

  # just to facilitate mocking
  def _get_user_list_service(self, customer_id):
    return ads_utils.get_ads_service('AdwordsUserListService', 'v201809',
                                     self.oauth_credentials,
                                     self.developer_token.get(), customer_id)

  def _assert_execution_is_valid(self, elements, any_execution) -> None:
    ads_utils.assert_elements_have_same_execution(elements)
    destination = any_execution.destination.destination_metadata

    # The number of parameters vary by upload. This test could be parameterized
    if not destination[0]:
      raise ValueError('Missing destination information. Received {}'.format(
          str(destination)))

  @utils.safe_process(logger=logging.getLogger(_DEFAULT_LOGGER))
  def process(self, elements, **kwargs) -> None:
    """Args:

       elements: List of dict with two elements: 'execution' and 'row'. All
       executions must be equal.
    """
    if not self.active:
      logging.getLogger(_DEFAULT_LOGGER).warning(
          'Skipping upload to ads, parameters not configured.')
      return

    any_execution = elements[0]['execution']
    if any_execution.destination.destination_type is self.get_action_type():
      self._assert_execution_is_valid(elements, any_execution)

      user_list_service = self._get_user_list_service(
          any_execution.account_config.google_ads_account_id)
      list_id = self._create_list_if_it_does_not_exist(
          user_list_service, any_execution.destination.destination_metadata[0],
          self.get_list_definition(
              any_execution.account_config,
              any_execution.destination.destination_metadata))

      rows = self.get_filtered_rows(
          utils.extract_rows(elements), self.get_row_keys())
      logging.getLogger(_DEFAULT_LOGGER).warning(
          'Uploading %d rows to Google Ads', len(rows))
      mutate_members_operation = {
          'operand': {
              'userListId': list_id,
              'membersList': rows
          },
          'operator': any_execution.destination.destination_metadata[1]
      }
      utils.safe_call_api(self.call_api, logging, user_list_service, [mutate_members_operation])
      logging.getLogger(_DEFAULT_LOGGER).warning(
        'Uploaded %d rows to Google Ads', len(rows))
    yield elements

  def call_api(self, service, operations):
    service.mutateMembers(operations)

  def get_filtered_rows(self, rows: List[Any],
                        keys: List[str]) -> List[Dict[str, Any]]:
    return [{key: row.get(key) for key in keys} for row in rows]

  def get_list_definition(self, account_config: AccountConfig,
                          list_name: str) -> Dict[str, Any]:
    pass

  def get_row_keys(self) -> List[str]:
    pass

  def get_action_type(self) -> DestinationType:
    pass
