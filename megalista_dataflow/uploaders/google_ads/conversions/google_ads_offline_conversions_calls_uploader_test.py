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
from unittest.mock import MagicMock

import pytest
from apache_beam.options.value_provider import StaticValueProvider

from error.error_handling import ErrorHandler
from error.error_handling_test import MockErrorNotifier
from models.execution import AccountConfig
from models.execution import Batch
from models.execution import Destination
from models.execution import DestinationType
from models.execution import Execution
from models.execution import Source
from models.execution import SourceType
from models.oauth_credentials import OAuthCredentials
from uploaders.google_ads.conversions.google_ads_offline_conversions_calls_uploader import GoogleAdsOfflineUploaderCallsDoFn

_account_config = AccountConfig('123-45567-890', False, 'ga_account_id', '', '')


@pytest.fixture()
def error_notifier():
  return MockErrorNotifier()


@pytest.fixture
def uploader(mocker, error_notifier):
  mocker.patch('google.ads.googleads.client.GoogleAdsClient')
  mocker.patch('google.ads.googleads.oauth2')
  credential_id = StaticValueProvider(str, 'id')
  secret = StaticValueProvider(str, 'secret')
  access = StaticValueProvider(str, 'access')
  refresh = StaticValueProvider(str, 'refresh')
  credentials = OAuthCredentials(credential_id, secret, access, refresh)
  return GoogleAdsOfflineUploaderCallsDoFn(credentials,
                                          StaticValueProvider(str, 'devtoken'),
                                          ErrorHandler(DestinationType.ADS_OFFLINE_CONVERSION_CALLS, error_notifier))


def test_get_service(mocker, uploader):
  assert uploader._get_oc_service(mocker.ANY) is not None


def arrange_conversion_resource_name_api_call(mocker, uploader, conversion_resource_name):
  mocker.patch.object(uploader, '_get_ads_service')

  resource_name_result = MagicMock()
  resource_name_result.conversion_action.resource_name = conversion_resource_name

  resource_name_batch_response = MagicMock()
  resource_name_batch_response.results = [resource_name_result]

  uploader._get_ads_service.return_value.search_stream.return_value = [resource_name_batch_response]


def test_conversion_upload(mocker, uploader):
  # arrange
  conversion_resource_name = 'user_list_resouce'
  arrange_conversion_resource_name_api_call(mocker, uploader, conversion_resource_name)

  mocker.patch.object(uploader, '_get_oc_service')
  conversion_name = 'user_list'
  destination = Destination(
    'dest1', DestinationType.ADS_OFFLINE_CONVERSION_CALLS, ['user_list'])
  source = Source('orig1', SourceType.BIG_QUERY, ['dt1', 'buyers'])
  execution = Execution(_account_config, source, destination)

  time1 = '2020-04-09T14:13:55.0005'
  time1_result = '2020-04-09 14:13:55-03:00'

  call_time1 = '2020-04-08T14:13:55.0005'
  call_time1_result = '2020-04-08 14:13:55-03:00'

  time2 = '2020-04-09T13:13:55.0005'
  time2_result = '2020-04-09 13:13:55-03:00'

  call_time2 = '2020-04-08T13:13:55.0005'
  call_time2_result = '2020-04-08 13:13:55-03:00'

  element1 = {
    'time': time1,
    'call_time': call_time1,
    'amount': '123',
    'caller_id': '+5511987654321'
  }
  element2 = {
    'time': time2,
    'call_time': call_time2,
    'amount': '234',
    'caller_id': '+5511987654321'
  }

  batch = Batch(execution, [element1, element2])

  # act
  uploader.process(batch)
  uploader.finish_bundle()

  uploader._get_ads_service.return_value.search_stream.assert_called_once_with(
    customer_id='12345567890',
    query=f"SELECT conversion_action.resource_name FROM conversion_action WHERE conversion_action.name = '{conversion_name}'"
  )

  uploader._get_oc_service.return_value.upload_call_conversions.assert_called_once_with(request={
    'customer_id': '12345567890',
    'partial_failure': True,
    'validate_only': False,
    'conversions': [{
      'conversion_action': conversion_resource_name,
      'call_start_date_time': call_time1_result,
      'conversion_date_time': time1_result,
      'conversion_value': 123,
      'caller_id': '+5511987654321'
    }, {
      'conversion_action': conversion_resource_name,
      'call_start_date_time': call_time2_result,
      'conversion_date_time': time2_result,
      'conversion_value': 234,
      'caller_id': '+5511987654321'
    }]
  })

def test_upload_with_ads_account_override(mocker, uploader):
  # arrange
  conversion_resource_name = 'user_list_resouce'
  arrange_conversion_resource_name_api_call(mocker, uploader, conversion_resource_name)

  mocker.patch.object(uploader, '_get_oc_service')
  conversion_name = 'user_list'
  destination = Destination(
    'dest1', DestinationType.ADS_OFFLINE_CONVERSION_CALLS, ['user_list', '987-7654-123'])
  source = Source('orig1', SourceType.BIG_QUERY, ['dt1', 'buyers'])
  execution = Execution(_account_config, source, destination)

  time1 = '2020-04-09T14:13:55.0005'
  time1_result = '2020-04-09 14:13:55-03:00'

  call_time1 = '2020-04-08T14:13:55.0005'
  call_time1_result = '2020-04-08 14:13:55-03:00'

  time2 = '2020-04-09T13:13:55.0005'
  time2_result = '2020-04-09 13:13:55-03:00'

  call_time2 = '2020-04-08T13:13:55.0005'
  call_time2_result = '2020-04-08 13:13:55-03:00'

  element1 = {
    'time': time1,
    'call_time': call_time1,
    'amount': '123',
    'caller_id': '+5511987654321'
  }
  element2 = {
    'time': time2,
    'call_time': call_time2,
    'amount': '234',
    'caller_id': '+5511987654321'
  }

  batch = Batch(execution, [element1, element2])

  # act
  uploader.process(batch)
  uploader.finish_bundle()

  # assert
  uploader._get_ads_service.return_value.search_stream.assert_called_once_with(
    customer_id='9877654123',
    query=f"SELECT conversion_action.resource_name FROM conversion_action WHERE conversion_action.name = '{conversion_name}'"
  )

  uploader._get_oc_service.return_value.upload_call_conversions.assert_called_once_with(request={
    'customer_id': '9877654123',
    'partial_failure': True,
    'validate_only': False,
    'conversions': [{
      'conversion_action': conversion_resource_name,
      'call_start_date_time': call_time1_result,
      'conversion_date_time': time1_result,
      'conversion_value': 123,
      'caller_id': '+5511987654321'
    }, {
      'conversion_action': conversion_resource_name,
      'call_start_date_time': call_time2_result,
      'conversion_date_time': time2_result,
      'conversion_value': 234,
      'caller_id': '+5511987654321'
    }]
  })


def test_should_not_notify_errors_when_api_call_is_successful(mocker, uploader, error_notifier):
  # arrange
  conversion_resource_name = 'user_list_resouce'
  arrange_conversion_resource_name_api_call(mocker, uploader, conversion_resource_name)

  mocker.patch.object(uploader, '_get_oc_service')
  conversion_name = 'user_list'
  destination = Destination(
    'dest1', DestinationType.ADS_OFFLINE_CONVERSION_CALLS, ['user_list'])
  source = Source('orig1', SourceType.BIG_QUERY, ['dt1', 'buyers'])
  execution = Execution(_account_config, source, destination)

  time1 = '2020-04-09T14:13:55.0005'
  
  call_time1 = '2020-04-08T14:13:55.0005'
  
  element1 = {
    'time': time1,
    'call_time': call_time1,
    'amount': '123',
    'caller_id': '+5511987654321'
  }
  batch = Batch(execution, [element1])

  result_mock1 = MagicMock()

  upload_return_mock = MagicMock()
  upload_return_mock.results = [result_mock1]
  upload_return_mock.partial_failure_error = None

  uploader._get_oc_service.return_value.upload_call_conversions.return_value = upload_return_mock

  # act
  uploader.process(batch)
  uploader.finish_bundle()

  uploader._get_ads_service.return_value.search_stream.assert_called_once_with(
    customer_id='12345567890',
    query=f"SELECT conversion_action.resource_name FROM conversion_action WHERE conversion_action.name = '{conversion_name}'"
  )

  assert error_notifier.were_errors_sent is False


def test_error_notification(mocker, uploader, error_notifier):
  # arrange
  conversion_resource_name = 'user_list_resouce'
  arrange_conversion_resource_name_api_call(mocker, uploader, conversion_resource_name)

  mocker.patch.object(uploader, '_get_oc_service')
  destination = Destination(
    'dest1', DestinationType.ADS_OFFLINE_CONVERSION_CALLS, ['user_list'])
  source = Source('orig1', SourceType.BIG_QUERY, ['dt1', 'buyers'])
  execution = Execution(_account_config, source, destination)

  time1 = '2020-04-09T14:13:55.0005'
  
  call_time1 = '2020-04-08T14:13:55.0005'
  
  element1 = {
    'time': time1,
    'call_time': call_time1,
    'amount': '123',
    'caller_id': '+5511987654321'
  }
  batch = Batch(execution, [element1])

  error_message = 'Offline Conversion uploading failures'
  upload_return_mock = MagicMock()
  upload_return_mock.partial_failure_error.message = error_message
  uploader._get_oc_service.return_value.upload_call_conversions.return_value = upload_return_mock

  # act
  uploader.process(batch)
  uploader.finish_bundle()

  # assert
  assert error_notifier.were_errors_sent is True
  assert error_notifier.destination_type is DestinationType.ADS_OFFLINE_CONVERSION_CALLS
  assert error_notifier.errors_sent == {execution: f'Error on uploading offline conversions (calls): {error_message}.'}


# def test_conversion_upload_and_error_notification(mocker, uploader, error_notifier):
#   """
#   Scenario where some gclids are uploaded but some give errors
#   """

#   # arrange
#   conversion_resource_name = 'user_list_resouce'
#   arrange_conversion_resource_name_api_call(mocker, uploader, conversion_resource_name)

#   mocker.patch.object(uploader, '_get_oc_service')
#   conversion_name = 'user_list'
#   destination = Destination(
#     'dest1', DestinationType.ADS_OFFLINE_CONVERSION, ['user_list'])
#   source = Source('orig1', SourceType.BIG_QUERY, ['dt1', 'buyers'])
#   execution = Execution(_account_config, source, destination)

#   time1 = '2020-04-09T14:13:55.0005'
#   time1_result = '2020-04-09 14:13:55-03:00'

#   time2 = '2020-04-09T13:13:55.0005'
#   time2_result = '2020-04-09 13:13:55-03:00'

#   element1 = {
#     'time': time1,
#     'amount': '123',
#     'gclid': '456'
#   }
#   element2 = {
#     'time': time2,
#     'amount': '234',
#     'gclid': '567'
#   }
#   batch = Batch(execution, [element1, element2])

#   # gclid '456' returns as successful by the API, while gclid '567' does not.
#   # in this scenario, it's expected that both are present in the API call,
#   # but since gclid '567' is not returned as successful by the API, an error is sent through error_notifier

#   error_message = 'Offline Conversion uploading failures'

#   gclid_result_mock1 = MagicMock()
#   gclid_result_mock1.gclid = '456'

#   upload_return_mock = MagicMock()
#   upload_return_mock.results = [gclid_result_mock1]
#   upload_return_mock.partial_failure_error.message = error_message
#   uploader._get_oc_service.return_value.upload_click_conversions.return_value = upload_return_mock

#   # act
#   successful_uploaded_gclids_batch = uploader.process(batch)[0]
#   uploader.finish_bundle()

#   # assert
#   assert len(successful_uploaded_gclids_batch.elements) == 1
#   assert successful_uploaded_gclids_batch.elements[0] == element1

#   uploader._get_ads_service.return_value.search_stream.assert_called_once_with(
#     customer_id='12345567890',
#     query=f"SELECT conversion_action.resource_name FROM conversion_action WHERE conversion_action.name = '{conversion_name}'"
#   )

#   uploader._get_oc_service.return_value.upload_click_conversions.assert_called_once_with(request={
#     'customer_id': '12345567890',
#     'partial_failure': True,
#     'validate_only': False,
#     'conversions': [{
#       'conversion_action': conversion_resource_name,
#       'conversion_date_time': time1_result,
#       'conversion_value': 123,
#       'gclid': '456'
#     }, {
#       'conversion_action': conversion_resource_name,
#       'conversion_date_time': time2_result,
#       'conversion_value': 234,
#       'gclid': '567'
#     }]
#   })

#   assert error_notifier.were_errors_sent is True
#   assert error_notifier.destination_type is DestinationType.ADS_OFFLINE_CONVERSION
#   assert error_notifier.errors_sent == {execution: f'Error on uploading offline conversions: {error_message}.'}
