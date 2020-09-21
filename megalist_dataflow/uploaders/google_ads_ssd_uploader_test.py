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

import pytest
from apache_beam.options.value_provider import StaticValueProvider

from uploaders.google_ads_ssd_uploader import GoogleAdsSSDUploaderDoFn
from megalist_dataflow.utils.execution import AccountConfig
from megalist_dataflow.utils.execution import Destination
from megalist_dataflow.utils.execution import DestinationType
from megalist_dataflow.utils.execution import Execution
from megalist_dataflow.utils.execution import Source
from megalist_dataflow.utils.execution import SourceType
from utils.oauth_credentials import OAuthCredentials

_account_config = AccountConfig('account_id', False, 'ga_account_id', '', '')


@pytest.fixture
def uploader(mocker):
  mocker.patch('googleads.oauth2.GoogleRefreshTokenClient')
  mocker.patch('googleads.adwords.AdWordsClient')
  id = StaticValueProvider(str, 'id')
  secret = StaticValueProvider(str, 'secret')
  access = StaticValueProvider(str, 'access')
  refresh = StaticValueProvider(str, 'refresh')
  credentials = OAuthCredentials(id, secret, access, refresh)
  return GoogleAdsSSDUploaderDoFn(credentials,
                                  StaticValueProvider(str, 'devtoken'))


def test_get_service(mocker, uploader):
  assert uploader._get_ssd_service(mocker.ANY) is not None


def test_not_active(mocker, caplog):
  id = StaticValueProvider(str, 'id')
  secret = StaticValueProvider(str, 'secret')
  access = StaticValueProvider(str, 'access')
  refresh = StaticValueProvider(str, 'refresh')
  credentials = OAuthCredentials(id, secret, access, refresh)
  uploader = GoogleAdsSSDUploaderDoFn(credentials, None)
  mocker.patch.object(uploader, '_get_ssd_service')
  uploader.process([{}],)
  uploader._get_ssd_service.assert_not_called()
  assert 'Skipping upload to ads, parameters not configured.' in caplog.text


def test_work_with_empty_elements(uploader, mocker, caplog):
  mocker.patch.object(uploader, '_get_ssd_service')
  uploader.process([],)
  uploader._get_ssd_service.assert_not_called()
  assert 'Skipping upload, received no elements.' in caplog.text


def test_fail_having_more_than_one_execution(uploader):
  source1 = Source('orig1', SourceType.BIG_QUERY, ('dt1', 'buyers'))
  destination1 = Destination(
      'dest1', DestinationType.ADS_SSD_UPLOAD, ('a'))
  exec1 = Execution(_account_config, source1, destination1)

  source2 = Source('origi2', SourceType.BIG_QUERY, ('dt2', 'buyers2'))
  destination2 = Destination(
      'dest2', DestinationType.ADS_SSD_UPLOAD, ('a'))
  exec2 = Execution(_account_config, source2, destination2)

  with pytest.raises(
      ValueError, match='At least two Execution in a single call'):
    uploader.process([{'execution': exec1}, {'execution': exec2}],)


def test_fail_with_wrong_destination_type(mocker, uploader):
  source = Source('orig1', SourceType.BIG_QUERY, ('dt1', 'buyers'))
  destination = Destination('dest1', DestinationType.GA_USER_LIST_UPLOAD, ('a'))
  execution = Execution(_account_config, source, destination)

  with pytest.raises(ValueError, match='Wrong DestinationType received'):
    uploader.process([{'execution': execution}],)


def test_fail_missing_destination_metadata(uploader):
  source = Source('orig1', SourceType.BIG_QUERY, ('dt1', 'buyers'))
  destination = Destination('dest1', DestinationType.ADS_SSD_UPLOAD, [])
  execution = Execution(_account_config, source, destination)

  with pytest.raises(ValueError, match='Missing destination information'):
    uploader.process([{'execution': execution}],)

  assert_empty_destination_metadata(uploader, (''))


def assert_empty_destination_metadata(uploader, destination_metadata):
  source = Source('orig1', SourceType.BIG_QUERY, ('dt1', 'buyers'))
  destination = Destination(
      'dest1', DestinationType.ADS_SSD_UPLOAD, destination_metadata)
  execution = Execution(_account_config, source, destination)
  with pytest.raises(ValueError, match='Missing destination information'):
    uploader.process([{'execution': execution}],)


def test_conversion_upload(mocker, uploader):
  mocker.patch.object(uploader, '_get_ssd_service')
  conversion_name = 'ssd_conversion'
  external_upload_id = '123'
  source = Source('orig1', SourceType.BIG_QUERY, ('dt1', 'buyers'))
  destination = Destination('dest1', DestinationType.ADS_SSD_UPLOAD,
                            [conversion_name, external_upload_id])
  execution = Execution(_account_config, source, destination)

  time1 = '2020-04-09T14:13:55.0005'
  time1_result = '20200409 141355 America/Sao_Paulo'

  time2 = '2020-04-09T13:13:55.0005'
  time2_result = '20200409 131355 America/Sao_Paulo'

  uploader.process([{
      'execution': execution,
      'row': {
          'hashedEmail': 'a@a.com',
          'time': time1,
          'amount': '123'
      }
  }, {
      'execution': execution,
      'row': {
          'hashedEmail': 'b@b.com',
          'time': time2,
          'amount': '234'
      }
  }],)

  upload_data = [{
      'StoreSalesTransaction': {
          'userIdentifiers': [{
              'userIdentifierType': 'HASHED_EMAIL',
              'value': 'a@a.com'
          }],
          'transactionTime': time1_result,
          'transactionAmount': {
              'currencyCode': 'BRL',
              'money': {
                  'microAmount': '123'
              }
          },
          'conversionName': conversion_name
      }
  }, {
      'StoreSalesTransaction': {
          'userIdentifiers': [{
              'userIdentifierType': 'HASHED_EMAIL',
              'value': 'b@b.com'
          }],
          'transactionTime': time2_result,
          'transactionAmount': {
              'currencyCode': 'BRL',
              'money': {
                  'microAmount': '234'
              }
          },
          'conversionName': conversion_name
      }
  }]

  uploader._get_ssd_service.return_value.mutate.assert_any_call([{
      'operand': {
          'externalUploadId': external_upload_id,
          'offlineDataList': upload_data,
          'uploadType': 'STORE_SALES_UPLOAD_FIRST_PARTY',
          'uploadMetadata': {
              'StoreSalesUploadCommonMetadata': {
                  'xsi_type': 'FirstPartyUploadMetadata',
                  'loyaltyRate': 1.0,
                  'transactionUploadRate': 1.0,
              }
          }
      },
      'operator': 'ADD'
  }])
