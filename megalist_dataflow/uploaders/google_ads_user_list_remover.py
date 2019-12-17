# Copyright 2019 Google LLC
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


class GoogleAdsUserListRemoverDoFn(beam.DoFn):
    crm_list_name = 'Megalist - CRM - Buyers'
    mobile_list_name = 'Megalist - Mobile - Buyers'
    rev_list_name = 'Megalist - Potential New Buyers'

    def __init__(self, oauth_credentials, developer_token, customer_id, app_id):
        self.oauth_credentials = oauth_credentials
        self.developer_token = developer_token
        self.customer_id = customer_id
        self.app_id = app_id
        self.active = True
        if self.developer_token is None or self.customer_id is None:
            self.active = False

    def _get_user_list_service(self):
        from googleads import adwords
        from googleads import oauth2
        oauth2_client = oauth2.GoogleRefreshTokenClient(
            self.oauth_credentials.get_client_id(), self.oauth_credentials.get_client_secret(), self.oauth_credentials.get_refresh_token())
        client = adwords.AdWordsClient(
            self.developer_token.get(), oauth2_client, 'MegaList Dataflow', client_customer_id=self.customer_id.get())
        return client.GetService(
            'AdwordsUserListService', 'v201809')

    def _create_list_if_it_does_not_exist(self, user_list_service, list_name, list_definition):
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
            logging.getLogger().info('%s created with id: %d' % (list_name, id))
        else:
            id = response.entries[0]['id']
            logging.getLogger().info('Found %s with id: %d' % (list_name, id))
        return id

    def start_bundle(self):
        if self.active == False:
            logging.getLogger().warning("Skipping upload to ads, parameters not configured.")
            return
        user_list_service = self._get_user_list_service()
        self.user_list_id = user_list_service.get([{
            'fields': ['Id', 'Name'],
            'predicates': [{
                'field': 'Name',
                'operator': 'EQUALS',
                'values': [self.crm_list_name]
            }]
        }]).entries[0]['id']

        self.mobile_user_list_id = user_list_service.get([{
            'fields': ['Id', 'Name'],
            'predicates': [{
                'field': 'Name',
                'operator': 'EQUALS',
                'values': [self.mobile_list_name]
            }]
        }]).entries[0]['id']

    def process(self, element):
        if self.active == False:
            return
        user_list_service = self._get_user_list_service()

        mobile_ids = [{'mobileId': row['mobileId']} for row in element]

        mutate_mobile_members_operation = {
            'operand': {
                'userListId': self.mobile_user_list_id,
                'membersList': mobile_ids
            },
            'operator': 'REMOVE'
        }
        user_list_service.mutateMembers([mutate_mobile_members_operation])

        for row in element:
            row.pop('mobileId', None)

        mutate_members_operation = {
            'operand': {
                'userListId': self.user_list_id,
                'membersList': element
            },
            'operator': 'REMOVE'
        }
        user_list_service.mutateMembers([mutate_members_operation])

        return element
