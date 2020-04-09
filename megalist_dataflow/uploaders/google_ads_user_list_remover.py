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

import apache_beam as beam
import logging

from uploaders import google_ads_utils as utils
from utils.execution import Action


class GoogleAdsUserListRemoverDoFn(beam.DoFn):

  def __init__(self, oauth_credentials, developer_token, customer_id, app_id):
    super().__init__()
    self.oauth_credentials = oauth_credentials
    self.developer_token = developer_token
    self.customer_id = customer_id
    self.app_id = app_id
    self.active = True
    if self.developer_token is None or self.customer_id is None:
      self.active = False
    self._user_list_id_cache = {}

  # just to facilitate mocking
  def _get_user_list_service(self):
    return utils.get_ads_service(
      'AdwordsUserListService', 'v201809', self.oauth_credentials, self.developer_token.get(), self.customer_id.get())

  def start_bundle(self):
    pass

  def _get_list_id(self, list_name):
    if self._user_list_id_cache.get(list_name) is None:
      self._user_list_id_cache[list_name] = self._do_get_list_id(list_name)

    return self._user_list_id_cache[list_name]

  def _do_get_list_id(self, list_name):
    user_list_service = self._get_user_list_service()
    return user_list_service.get([{
      'fields': ['Id', 'Name'],
      'predicates': [{
        'field': 'Name',
        'operator': 'EQUALS',
        'values': [list_name]
      }]
    }]).entries[0]['id']

  @staticmethod
  def _assert_all_list_names_are_present(any_execution):
    destination = any_execution.destination_metadata
    if len(destination) is not 2:
      raise ValueError('Missing destination information. Found {}'.format(len(destination)))

    if not destination[0] \
        or not destination[1]:
      raise ValueError('Missing destination information. Received {}'.format(str(destination)))

  def process(self, elements, **kwargs):
    if not self.active:
      logging.getLogger().warning("Skipping upload to ads, parameters not configured.")
      return

    if len(elements) == 0:
      logging.getLogger().warning('Skipping upload to ads, received no elements.')
      return

    utils.assert_elements_have_same_execution(elements)
    any_execution = elements[0]['execution']
    utils.assert_right_type_action(any_execution, Action.ADS_USER_LIST_REMOVE)
    self._assert_all_list_names_are_present(any_execution)

    user_list_id = self._get_list_id(any_execution.destination_metadata[0])
    mobile_user_list_id = self._get_list_id(any_execution.destination_metadata[1])

    user_list_service = self._get_user_list_service()

    self._do_upload(user_list_service, self._extract_rows(elements), user_list_id, mobile_user_list_id)

  @staticmethod
  def _extract_rows(elements):
    return [dict['row'] for dict in elements]

  @staticmethod
  def _do_upload(user_list_service, rows, user_list_id, mobile_user_list_id):

    mobile_ids = [{'mobileId': row['mobileId']} for row in rows]

    mutate_mobile_members_operation = {
      'operand': {
        'userListId': mobile_user_list_id,
        'membersList': mobile_ids
      },
      'operator': 'REMOVE'
    }
    user_list_service.mutateMembers([mutate_mobile_members_operation])

    for row in rows:
      row.pop('mobileId', None)

    mutate_members_operation = {
      'operand': {
        'userListId': user_list_id,
        'membersList': rows
      },
      'operator': 'REMOVE'
    }
    user_list_service.mutateMembers([mutate_members_operation])
