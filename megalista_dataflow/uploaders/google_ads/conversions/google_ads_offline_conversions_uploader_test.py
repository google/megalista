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

from apache_beam.options.value_provider import StaticValueProvider
import pytest

from uploaders.google_ads.conversions.google_ads_offline_conversions_uploader import GoogleAdsOfflineUploaderDoFn
from models.execution import AccountConfig
from models.execution import Destination
from models.execution import DestinationType
from models.execution import Execution
from models.execution import Source
from models.execution import SourceType
from models.execution import Batch
from models.oauth_credentials import OAuthCredentials

_account_config = AccountConfig('123-45567-890', False, 'ga_account_id', '', '')


@pytest.fixture
def uploader(mocker):
  mocker.patch('google.ads.googleads.client.GoogleAdsClient')
  mocker.patch('google.ads.googleads.oauth2')
  credential_id = StaticValueProvider(str, 'id')
  secret = StaticValueProvider(str, 'secret')
  access = StaticValueProvider(str, 'access')
  refresh = StaticValueProvider(str, 'refresh')
  credentials = OAuthCredentials(credential_id, secret, access, refresh)
  return GoogleAdsOfflineUploaderDoFn(credentials,
                                      StaticValueProvider(str, 'devtoken'))


def test_get_service(mocker, uploader):
  assert uploader._get_oc_service(mocker.ANY) is not None


def test_conversion_upload(mocker, uploader):
  # arrange
  mocker.patch.object(uploader, '_get_ads_service')

  conversion_resource_name = 'user_list_resouce'
  resource_name_result = MagicMock()
  resource_name_result.conversion_action.resource_name = conversion_resource_name

  resource_name_batch_response = MagicMock()
  resource_name_batch_response.results = [resource_name_result]

  uploader._get_ads_service.return_value.search_stream.return_value = [resource_name_batch_response]

  # act
  mocker.patch.object(uploader, '_get_oc_service')
  conversion_name = 'user_list'
  destination = Destination(
      'dest1', DestinationType.ADS_OFFLINE_CONVERSION, ['user_list'])
  source = Source('orig1', SourceType.BIG_QUERY, ['dt1', 'buyers'])
  execution = Execution(_account_config, source, destination)

  time1 = '2020-04-09T14:13:55.0005'
  time1_result = '2020-04-09 14:13:55-03:00'

  time2 = '2020-04-09T13:13:55.0005'
  time2_result = '2020-04-09 13:13:55-03:00'

  batch = Batch(execution, [{
          'time': time1,
          'amount': '123',
          'gclid': '456'
      },{
          'time': time2,
          'amount': '234',
          'gclid': '567'
      }])
  uploader.process(batch)

  #assert
  uploader._get_ads_service.return_value.search_stream.assert_called_once_with(
    customer_id='12345567890',
    query=f"SELECT conversion_action.resource_name FROM conversion_action WHERE conversion_action.name = '{conversion_name}'"
  )

  uploader._get_oc_service.return_value.upload_click_conversions.assert_called_once_with(request={
    'customer_id': '12345567890',
    'partial_failure': True,
    'validate_only': False,
    'conversions': [{
      'conversion_action': conversion_resource_name,
      'conversion_date_time': time1_result,
      'conversion_value': 123,
      'gclid': '456'
    }, {
      'conversion_action': conversion_resource_name,
      'conversion_date_time': time2_result,
      'conversion_value': 234,
      'gclid': '567'
    }]
  })
