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


class GoogleAdsUserListUploaderDoFn(beam.DoFn):

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

  def start_bundle(self):
    pass

  def _create_list_if_it_does_not_exist(self, user_list_service, list_name, list_definition):
    if self._user_list_id_cache.get(list_name) is None:
      self._user_list_id_cache[list_name] = \
        self._do_create_list_if_it_does_not_exist(user_list_service, list_name, list_definition)

    return self._user_list_id_cache[list_name]

  @staticmethod
  def _do_create_list_if_it_does_not_exist(user_list_service, list_name, list_definition):
    response = user_list_service.get([{
      'fields': ['Id', 'Name'],
      'predicates': [{
        'field': 'Name',
        'operator': 'EQUALS',
        'values': [list_name]
      }]
    }])
    if (len(response.entries) == 0):
      logging.getLogger().info('%s list does not exist, creating...' % list_name)
      result = user_list_service.mutate([{
        'operator': 'ADD',
        **list_definition
      }])
      id = result['value'][0]['id']
      logging.getLogger().info('List %s created with id: %d' % (list_name, id))
    else:
      id = response.entries[0]['id']
      logging.getLogger().info('List found %s with id: %d' % (list_name, id))
    return id

  # just to facilitate mocking
  def _get_user_list_service(self):
    return utils.get_ads_service(
      'AdwordsUserListService', 'v201809', self.oauth_credentials, self.developer_token.get(), self.customer_id.get())

  def _create_lists(self, crm_list_name, mobile_list_name, rev_list_name):
    return self._do_create_lists(
      self._get_user_list_service(),
      self.app_id.get(),
      crm_list_name,
      mobile_list_name,
      rev_list_name)

  def _do_create_lists(self, user_list_service, app_id, crm_list_name, mobile_list_name, rev_list_name):

    user_list_id = self._create_list_if_it_does_not_exist(user_list_service, crm_list_name, {
      'operand': {
        'xsi_type': 'CrmBasedUserList',
        'name': crm_list_name,
        'description': crm_list_name,
        # CRM-based user lists can use a membershipLifeSpan of 10000 to indicate
        # unlimited; otherwise normal values apply.
        'membershipLifeSpan': 10000,
        'uploadKeyType': 'CONTACT_INFO'
      }
    })

    mobile_user_list_id = self._create_list_if_it_does_not_exist(user_list_service, mobile_list_name, {
      'operand': {
        'xsi_type': 'CrmBasedUserList',
        'name': mobile_list_name,
        'description': mobile_list_name,
        # CRM-based user lists can use a membershipLifeSpan of 10000 to indicate
        # unlimited; otherwise normal values apply.
        'membershipLifeSpan': 10000,
        'appId': app_id,
        'uploadKeyType': 'MOBILE_ADVERTISING_ID'
      }
    })

    self._create_list_if_it_does_not_exist(user_list_service, rev_list_name, {
      'operand': {
        'xsi_type': 'LogicalUserList',
        'name': rev_list_name,
        'description': rev_list_name,
        'status': 'OPEN',
        'rules': [{
          'operator': 'NONE',
          'ruleOperands': [{
            'UserList': {
              'id': user_list_id,
              'xsi_type': 'CrmBasedUserList'
            }
          }, {
            'UserList': {
              'id': mobile_user_list_id,
              'xsi_type': 'CrmBasedUserList'
            }}]
        }]
      }
    })

    return user_list_id, mobile_user_list_id

  @staticmethod
  def _assert_all_list_names_are_present(any_execution):
    destination = any_execution.destination_metadata
    if len(destination) is not 3:
      raise ValueError('Missing destination information. Found {}'.format(len(destination)))

    if not destination[0] \
        or not destination[1] \
        or not destination[2]:
      raise ValueError('Missing destination information. Received {}'.format(str(destination)))

  def process(self, elements, **kwargs):
    """
    Args:
       elements: List of dict with two elements: 'execution' and 'row'. All executions must be equal.
    """
    if not self.active:
      logging.getLogger().warning('Skipping upload to ads, parameters not configured.')
      return

    if len(elements) == 0:
      logging.getLogger().warning('Skipping upload to ads, received no elements.')
      return

    utils.assert_elements_have_same_execution(elements)
    any_execution = elements[0]['execution']
    utils.assert_right_type_action(any_execution, Action.ADS_USER_LIST_UPLOAD)
    self._assert_all_list_names_are_present(any_execution)

    user_list_id, mobile_user_list_id = self._create_lists(any_execution.destination_metadata[0],
                                                           any_execution.destination_metadata[1],
                                                           any_execution.destination_metadata[2])

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
      'operator': 'ADD'
    }

    user_list_service.mutateMembers([mutate_mobile_members_operation])

    for row in rows:
      row.pop('mobileId', None)

    mutate_members_operation = {
      'operand': {
        'userListId': user_list_id,
        'membersList': rows
      },
      'operator': 'ADD'
    }

    user_list_service.mutateMembers([mutate_members_operation])
