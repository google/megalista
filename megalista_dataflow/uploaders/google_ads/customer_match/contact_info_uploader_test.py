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

import pytest
from apache_beam.options.value_provider import StaticValueProvider

from models.execution import AccountConfig, Destination, DestinationType, Source, SourceType, Execution, Batch
from models.oauth_credentials import OAuthCredentials
from uploaders.google_ads.customer_match.contact_info_uploader import GoogleAdsCustomerMatchContactInfoUploaderDoFn

_account_config = AccountConfig('account_id', False, 'ga_account_id', '', '')
_mcc_account_config = AccountConfig('mcc_account_id', True, 'ga_account_id', '', '')


@pytest.fixture
def uploader(mocker):
  mocker.patch('google.ads.googleads.client.GoogleAdsClient')
  mocker.patch('google.ads.googleads.oauth2')
  id = StaticValueProvider(str, 'id')
  secret = StaticValueProvider(str, 'secret')
  access = StaticValueProvider(str, 'access')
  refresh = StaticValueProvider(str, 'refresh')
  credentials = OAuthCredentials(id, secret, access, refresh)
  return GoogleAdsCustomerMatchContactInfoUploaderDoFn(credentials,
                                                       StaticValueProvider(str, 'devtoken'))


def test_upload_add_users(mocker, uploader):
  mocker.patch.object(uploader, '_get_offline_user_data_job_service')

  uploader._get_offline_user_data_job_service.return_value.create_offline_user_data_job.return_value.resource_name = 'a'


  destination = Destination(
    'dest1', DestinationType.ADS_CUSTOMER_MATCH_CONTACT_INFO_UPLOAD, ['user_list', 'ADD'])
  source = Source('orig1', SourceType.BIG_QUERY, ['dt1', 'buyers'])
  execution = Execution(_account_config, source, destination)

  batch = Batch(execution, [{
    'hashed_email': 'email1',
    'hashed_phone_number': 'phone1',
    'address_info': {
      'hashed_first_name': 'first1',
      'hashed_last_name': 'last1',
      'country_code': 'country1',
      'postal_code': 'postal1',
    }
  }])

  uploader.process(batch)

  data_insertion_payload = {
    'enable_partial_failure': False,
    'operations': [
      {'create': {'user_identifiers': [{'hashed_email': 'email1'}]}},
      {'create': {'user_identifiers': [{'address_info': {
        'hashed_first_name': 'first1',
        'hashed_last_name': 'last1',
        'country_code': 'country1',
        'postal_code': 'postal1'}},
      ]}},
      {'create': {'user_identifiers': [{'hashed_phone_number': 'phone1'}]}}
    ],
    'resource_name': 'a',
  }

  uploader._get_offline_user_data_job_service.assert_called_once_with('account_id') 
  uploader._get_offline_user_data_job_service.return_value.add_offline_user_data_job_operations.assert_called_once_with(
    request=data_insertion_payload
  )

def test_upload_replace_users(mocker, uploader):
  mocker.patch.object(uploader, '_get_offline_user_data_job_service')

  uploader._get_offline_user_data_job_service.return_value.create_offline_user_data_job.return_value.resource_name = 'a'


  destination = Destination(
    'dest1', DestinationType.ADS_CUSTOMER_MATCH_CONTACT_INFO_UPLOAD, ['user_list', 'REPLACE'])
  source = Source('orig1', SourceType.BIG_QUERY, ['dt1', 'buyers'])
  execution = Execution(_account_config, source, destination)

  batch = Batch(execution, [{
    'hashed_email': 'email1',
    'hashed_phone_number': 'phone1',
    'address_info': {
      'hashed_first_name': 'first1',
      'hashed_last_name': 'last1',
      'country_code': 'country1',
      'postal_code': 'postal1',
    }
  }])

  uploader.process(batch)

  data_insertion_payload = {
    'enable_partial_failure': False,
    'operations': [
      {'remove_all': True},
      {'create': {'user_identifiers': [{'hashed_email': 'email1'}]}},
      {'create': {'user_identifiers': [{'address_info': {
        'hashed_first_name': 'first1',
        'hashed_last_name': 'last1',
        'country_code': 'country1',
        'postal_code': 'postal1'}},
      ]}},
      {'create': {'user_identifiers': [{'hashed_phone_number': 'phone1'}]}}
    ],
    'resource_name': 'a',
  }

  uploader._get_offline_user_data_job_service.assert_called_once_with('account_id') 
  uploader._get_offline_user_data_job_service.return_value.add_offline_user_data_job_operations.assert_called_once_with(
    request=data_insertion_payload
  )

def test_upload_add_users_with_ads_account_override(mocker, uploader):
  mocker.patch.object(uploader, '_get_offline_user_data_job_service')

  uploader._get_offline_user_data_job_service.return_value.create_offline_user_data_job.return_value.resource_name = 'a'

  destination = Destination(
    'dest1', DestinationType.ADS_CUSTOMER_MATCH_CONTACT_INFO_UPLOAD, ['user_list', 'ADD', 'FALSE', '', 'override_account_id', ''])
  source = Source('orig1', SourceType.BIG_QUERY, ['dt1', 'buyers'])
  execution = Execution(_account_config, source, destination)

  batch = Batch(execution, [{
    'hashed_email': 'email1',
    'hashed_phone_number': 'phone1',
    'address_info': {
      'hashed_first_name': 'first1',
      'hashed_last_name': 'last1',
      'country_code': 'country1',
      'postal_code': 'postal1',
    }
  }])

  uploader.process(batch)

  data_insertion_payload = {
    'enable_partial_failure': False,
    'operations': [
      {'create': {'user_identifiers': [{'hashed_email': 'email1'}]}},
      {'create': {'user_identifiers': [{'address_info': {
        'hashed_first_name': 'first1',
        'hashed_last_name': 'last1',
        'country_code': 'country1',
        'postal_code': 'postal1'}},
      ]}},
      {'create': {'user_identifiers': [{'hashed_phone_number': 'phone1'}]}}
    ],
    'resource_name': 'a',
  }

  uploader._get_offline_user_data_job_service.assert_called_once_with('override_account_id') 
  uploader._get_offline_user_data_job_service.return_value.add_offline_user_data_job_operations.assert_called_once_with(
    request=data_insertion_payload
  )

def test_upload_add_users_with_mcc_account_override(mocker, uploader):
  mocker.patch.object(uploader, '_get_offline_user_data_job_service')

  uploader._get_offline_user_data_job_service.return_value.create_offline_user_data_job.return_value.resource_name = 'a'

  destination = Destination(
    'dest1', DestinationType.ADS_CUSTOMER_MATCH_CONTACT_INFO_UPLOAD, ['user_list', 'ADD', 'FALSE', '', 'override_account_id', ''])
  source = Source('orig1', SourceType.BIG_QUERY, ['dt1', 'buyers'])
  execution = Execution(_mcc_account_config, source, destination)

  batch = Batch(execution, [{
    'hashed_email': 'email1',
    'hashed_phone_number': 'phone1',
    'address_info': {
      'hashed_first_name': 'first1',
      'hashed_last_name': 'last1',
      'country_code': 'country1',
      'postal_code': 'postal1',
    }
  }])

  uploader.process(batch)

  data_insertion_payload = {
    'enable_partial_failure': False,
    'operations': [
      {'create': {'user_identifiers': [{'hashed_email': 'email1'}]}},
      {'create': {'user_identifiers': [{'address_info': {
        'hashed_first_name': 'first1',
        'hashed_last_name': 'last1',
        'country_code': 'country1',
        'postal_code': 'postal1'}},
      ]}},
      {'create': {'user_identifiers': [{'hashed_phone_number': 'phone1'}]}}
    ],
    'resource_name': 'a',
  }

  uploader._get_offline_user_data_job_service.assert_called_once_with('mcc_account_id') 
  uploader._get_offline_user_data_job_service.return_value.add_offline_user_data_job_operations.assert_called_once_with(
    request=data_insertion_payload
  )