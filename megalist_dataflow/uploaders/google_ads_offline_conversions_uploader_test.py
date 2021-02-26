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

from apache_beam.options.value_provider import StaticValueProvider
import pytest
from uploaders.google_ads_offline_conversions_uploader import GoogleAdsOfflineUploaderDoFn
from utils.execution import AccountConfig
from utils.execution import Destination
from utils.execution import DestinationType
from utils.execution import Execution
from utils.execution import Source
from utils.execution import SourceType
from utils.execution import Batch
from utils.oauth_credentials import OAuthCredentials

_account_config = AccountConfig('account_id', False, 'ga_account_id', '', '')


@pytest.fixture
def uploader(mocker):
  mocker.patch('googleads.oauth2.GoogleRefreshTokenClient')
  mocker.patch('googleads.adwords.AdWordsClient')
  credential_id = StaticValueProvider(str, 'id')
  secret = StaticValueProvider(str, 'secret')
  access = StaticValueProvider(str, 'access')
  refresh = StaticValueProvider(str, 'refresh')
  credentials = OAuthCredentials(credential_id, secret, access, refresh)
  return GoogleAdsOfflineUploaderDoFn(credentials,
                                      StaticValueProvider(str, 'devtoken'))


def test_get_service(mocker, uploader):
  assert uploader._get_oc_service(mocker.ANY) is not None


def test_not_active(mocker, caplog):
  credential_id = StaticValueProvider(str, 'id')
  secret = StaticValueProvider(str, 'secret')
  access = StaticValueProvider(str, 'access')
  refresh = StaticValueProvider(str, 'refresh')
  credentials = OAuthCredentials(credential_id, secret, access, refresh)
  uploader_dofn = GoogleAdsOfflineUploaderDoFn(credentials, None)
  mocker.patch.object(uploader_dofn, '_get_oc_service')
  uploader_dofn.process(Batch(None, []))
  uploader_dofn._get_oc_service.assert_not_called()
  assert 'Skipping upload, parameters not configured.' in caplog.text


def test_conversion_upload(mocker, uploader):
  mocker.patch.object(uploader, '_get_oc_service')
  conversion_name = 'user_list'
  destination = Destination(
      'dest1', DestinationType.ADS_OFFLINE_CONVERSION, ['user_list'])
  source = Source('orig1', SourceType.BIG_QUERY, ['dt1', 'buyers'])
  execution = Execution(_account_config, source, destination)

  time1 = '2020-04-09T14:13:55.0005'
  time1_result = '20200409 141355 America/Sao_Paulo'

  time2 = '2020-04-09T13:13:55.0005'
  time2_result = '20200409 131355 America/Sao_Paulo'

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

  uploader._get_oc_service.return_value.mutate.assert_any_call([{
      'operator': 'ADD',
      'operand': {
          'conversionName': conversion_name,
          'conversionTime': time1_result,
          'conversionValue': '123',
          'googleClickId': '456'
      }
  }, {
      'operator': 'ADD',
      'operand': {
          'conversionName': conversion_name,
          'conversionTime': time2_result,
          'conversionValue': '234',
          'googleClickId': '567'
      }
  }])
