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

from megalist_dataflow.uploaders.google_analytics_user_list_uploader import GoogleAnalyticsUserListUploaderDoFn
from megalist_dataflow.utils.oauth_credentials import OAuthCredentials
from utils.execution import Execution, SourceType, DestinationType, Source, AccountConfig, Destination


@pytest.fixture
def uploader(mocker):
  mocker.patch('googleads.oauth2.GoogleRefreshTokenClient')
  mocker.patch('googleads.adwords.AdWordsClient')
  client_id = StaticValueProvider(str, 'id')
  secret = StaticValueProvider(str, 'secret')
  access = StaticValueProvider(str, 'access')
  refresh = StaticValueProvider(str, 'refresh')
  credentials = OAuthCredentials(client_id, secret, access, refresh)
  return GoogleAnalyticsUserListUploaderDoFn(credentials)


def test_get_service(uploader):
  assert uploader._get_analytics_service() is not None


def test_work_with_empty_elements(uploader, mocker, caplog):
  mocker.patch.object(uploader, '_get_analytics_service')
  uploader.process([],)
  uploader._get_analytics_service.assert_not_called()
  assert 'Skipping upload, received no elements.' in caplog.text


def test_fail_having_more_than_one_execution(uploader, caplog):
  exec1 = Execution(
      AccountConfig('', False, '', '', ''),
      Source('orig1', SourceType.BIG_QUERY, ['dt1', 'buyers']),
      Destination('dest1', DestinationType.GA_USER_LIST_UPLOAD,
                  ['a', 'b', 'c', 'd', 'e', 'f']))
  exec2 = Execution(
      AccountConfig('', False, '', '', ''),
      Source('orig2', SourceType.BIG_QUERY, ['dt2', 'buyers2']),
      Destination('dest2', DestinationType.GA_USER_LIST_UPLOAD,
                  ['a', 'b', 'c', 'd', 'e', 'f']))

  with pytest.raises(ValueError,
                     match='At least two Execution in a single call'):
    uploader.process([{
        'execution': exec1,
        'row': ()
    }, {
        'execution': exec2,
        'row': ()
    }],)


def test_fail_with_wrong_action(uploader, caplog):
  execution = Execution(
      AccountConfig('', False, '', '', ''),
      Source('orig1', SourceType.BIG_QUERY, ['dt1', 'buyers']),
      Destination('dest1', DestinationType.ADS_SSD_UPLOAD,
                  ['a', 'b', 'c', 'd', 'e', 'f']))

  with pytest.raises(ValueError, match='Wrong DestinationType received'):
    uploader.process([{'execution': execution, 'row': ()}],)


def test_fail_missing_destination_metadata(uploader, caplog):
  execution = Execution(
      AccountConfig('', False, '', '', ''),
      Source('orig1', SourceType.BIG_QUERY, ['dt1', 'buyers']),
      Destination('dest1', DestinationType.GA_USER_LIST_UPLOAD, ['a', 'b']))

  with pytest.raises(ValueError,
                     match='Missing destination information'):
    uploader.process([{'execution': execution, 'row': ()}],)


def test_empty_destination_metadata(uploader, caplog):
  execution = Execution(
      AccountConfig('', False, '', '', ''),
      Source('orig1', SourceType.BIG_QUERY, ['dt1', 'buyers']),
      Destination('dest1', DestinationType.GA_USER_LIST_UPLOAD,
                  ('a', '', 'c', 'd', 'e', 'f')))
  with pytest.raises(ValueError,
                     match='Missing destination information'):
    uploader.process([{'execution': execution, 'row': ()}],)


def test_list_already_exists(mocker, uploader):
  service = mocker.MagicMock()
  service.management().remarketingAudience().list().execute = mocker.Mock(
      return_value={'items': [{
          'id': 1,
          'name': 'list'
      }]})

  mocker.patch.object(uploader, '_get_analytics_service')
  uploader._get_analytics_service.return_value = service

  execution = Execution(
      AccountConfig('', False, '', '', ''),
      Source('orig1', SourceType.BIG_QUERY, ['dt1', 'buyers']),
      Destination('dest1', DestinationType.GA_USER_LIST_UPLOAD,
                  ['a', 'b', 'c', 'list', 'd', 'e']))

  uploader.process([{'execution': execution, 'row': ()}])

  uploader._get_analytics_service().management().remarketingAudience(
  ).insert.assert_not_called()


def test_list_creation_not_mcc(mocker, uploader):
  ads_account_id = 'xxx-yyy-zzzz'
  ga_account_id = 'acc'

  service = mocker.MagicMock()

  mocker.patch.object(uploader, '_get_analytics_service')
  uploader._get_analytics_service.return_value = service

  service.management().remarketingAudience().insert().execute.return_value = {
      'id': 1
  }

  execution = Execution(
      AccountConfig(ads_account_id, False, ga_account_id, '', ''),
      Source('orig1', SourceType.BIG_QUERY, ['dt1', 'buyers']),
      Destination(
          'dest1', DestinationType.GA_USER_LIST_UPLOAD,
          ['web_property', 'view', 'c', 'list', 'd', 'buyers_custom_dim']))
  uploader.process([{'execution': execution, 'row': ()}])

  service.management().remarketingAudience().insert.assert_any_call(
      accountId=ga_account_id,
      webPropertyId='web_property',
      body={
          'name': 'list',
          'linkedViews': ['view'],
          'linkedAdAccounts': [{
              'type': 'ADWORDS_LINKS',
              'linkedAccountId': ads_account_id
          }],
          'audienceType': 'SIMPLE',
          'audienceDefinition': {
              'includeConditions': {
                  'kind':
                      'analytics#includeConditions',
                  'isSmartList':
                      False,
                  'segment':
                      'users::condition::%s==buyer' % 'buyers_custom_dim',
                  'membershipDurationDays':
                      365
              }
          }
      })


def test_list_creation_mcc(mocker, uploader):
  ads_account_id = 'xxx-yyy-zzzz'
  ga_account_id = 'acc'

  service = mocker.MagicMock()

  mocker.patch.object(uploader, '_get_analytics_service')
  uploader._get_analytics_service.return_value = service

  service.management().remarketingAudience().insert().execute.return_value = {
      'id': 1
  }

  execution = Execution(
      AccountConfig(ads_account_id, True, ga_account_id, '', ''),
      Source('orig1', SourceType.BIG_QUERY, ['dt1', 'buyers']),
      Destination(
          'dest1', DestinationType.GA_USER_LIST_UPLOAD,
          ['web_property', 'view', 'c', 'list', 'd', 'buyers_custom_dim']))
  uploader.process([{'execution': execution, 'row': ()}])

  service.management().remarketingAudience().insert.assert_any_call(
      accountId=ga_account_id,
      webPropertyId='web_property',
      body={
          'name': 'list',
          'linkedViews': ['view'],
          'linkedAdAccounts': [{
              'type': 'MCC_LINKS',
              'linkedAccountId': ads_account_id
          }],
          'audienceType': 'SIMPLE',
          'audienceDefinition': {
              'includeConditions': {
                  'kind':
                      'analytics#includeConditions',
                  'isSmartList':
                      False,
                  'segment':
                      'users::condition::%s==buyer' % 'buyers_custom_dim',
                  'membershipDurationDays':
                      365
              }
          }
      })


def test_avoid_list_creation_when_name_blank(mocker, uploader):
  ads_account_id = 'xxx-yyy-zzzz'
  ga_account_id = 'acc'

  service = mocker.MagicMock()

  mocker.patch.object(uploader, '_get_analytics_service')
  uploader._get_analytics_service.return_value = service

  execution = Execution(
      AccountConfig(ads_account_id, True, ga_account_id, '', ''),
      Source('orig1', SourceType.BIG_QUERY, ['dt1', 'buyers']),
      Destination('dest1', DestinationType.GA_USER_LIST_UPLOAD,
                  ['web_property', 'view', 'c', '', 'd', 'buyers_custom_dim']))

  uploader.process([{'execution': execution, 'row': ()}])

  service.management().remarketingAudience().insert.assert_not_called()


def test_elements_uploading(mocker, uploader):
  service = mocker.MagicMock()

  mocker.patch.object(uploader, '_get_analytics_service')
  uploader._get_analytics_service.return_value = service

  service.management().customDataSources().list().execute.return_value = {
      'items': [{
          'id': 1,
          'name': 'data_import_name'
      }]
  }

  execution = Execution(
      AccountConfig('', False, '', '', ''),
      Source('orig1', SourceType.BIG_QUERY, ['dt1', 'buyers']),
      Destination('dest1', DestinationType.GA_USER_LIST_UPLOAD, [
          'web_property', 'b', 'data_import_name', 'd', 'user_id_custom_dim',
          'buyer_custom_dim'
      ]))

  # Add mock to side effect of uploadData()
  my_mock = mocker.MagicMock()
  service.management().uploads().uploadData.side_effect = my_mock

  # Act
  uploader.process([{
      'execution': execution,
      'row': {
          'user_id': '12'
      }
  }, {
      'execution': execution,
      'row': {
          'user_id': '34'
      }
  }])

  # Called once
  my_mock.assert_called_once()

  # Intercept args called
  _, kwargs = my_mock.call_args

  # Check if really sent values from custom field
  media_bytes = kwargs['media_body'].getbytes(0, -1)

  assert media_bytes == b'user_id_custom_dim,buyer_custom_dim\n12,buyer\n34,buyer'


def test_elements_uploading_custom_field(mocker, uploader):
  service = mocker.MagicMock()

  mocker.patch.object(uploader, '_get_analytics_service')
  uploader._get_analytics_service.return_value = service

  service.management().customDataSources().list().execute.return_value = {
      'items': [{
          'id': 1,
          'name': 'data_import_name'
      }]
  }

  execution = Execution(
      AccountConfig('', False, '', '', ''),
      Source('orig1', SourceType.BIG_QUERY, ['dt1', 'buyers']),
      Destination('dest1', DestinationType.GA_USER_LIST_UPLOAD, [
          'web_property', 'b', 'data_import_name', 'd', 'user_id_custom_dim',
          'buyer_custom_dim', 'my_field'
      ]))

  # Add mock to side effect of uploadData()
  my_mock = mocker.MagicMock()
  service.management().uploads().uploadData.side_effect = my_mock

  # Act
  uploader.process([{
      'execution': execution,
      'row': {
          'user_id': '12',
          'my_field': '11'
      }
  }, {
      'execution': execution,
      'row': {
          'user_id': '34',
          'my_field': '22'
      }
  }])

  # Called once
  my_mock.assert_called_once()

  # Intercept args called
  _, kwargs = my_mock.call_args

  # Check if really sent values from custom field
  media_bytes = kwargs['media_body'].getbytes(0, -1)

  assert media_bytes == b'user_id_custom_dim,buyer_custom_dim\n12,11\n34,22'
