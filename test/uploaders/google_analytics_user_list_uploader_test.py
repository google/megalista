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
from googleapiclient.http import MediaInMemoryUpload

from megalist_dataflow.uploaders.google_analytics_user_list_uploader import GoogleAnalyticsUserListUploaderDoFn
from megalist_dataflow.utils.oauth_credentials import OAuthCredentials
from apache_beam.options.value_provider import StaticValueProvider
import pytest

from utils.execution import Execution, SourceType, Action


@pytest.fixture
def uploader(mocker):
  mocker.patch('googleads.oauth2.GoogleRefreshTokenClient')
  mocker.patch('googleads.adwords.AdWordsClient')
  id = StaticValueProvider(str, "id")
  secret = StaticValueProvider(str, "secret")
  access = StaticValueProvider(str, "access")
  refresh = StaticValueProvider(str, "refresh")
  credentials = OAuthCredentials(id, secret, access, refresh)
  account_id = StaticValueProvider(str, "acc")
  google_ads_account = StaticValueProvider(str, "xxx-yyy-zzzz")
  return GoogleAnalyticsUserListUploaderDoFn(credentials, account_id, google_ads_account)


def test_get_service(mocker, uploader):
  assert uploader._get_analytics_service() != None


def test_not_active(mocker):
  id = StaticValueProvider(str, "id")
  secret = StaticValueProvider(str, "secret")
  access = StaticValueProvider(str, "access")
  refresh = StaticValueProvider(str, "refresh")
  credentials = OAuthCredentials(id, secret, access, refresh)
  uploader = GoogleAnalyticsUserListUploaderDoFn(credentials, None, None)
  mocker.patch.object(uploader, '_get_analytics_service')
  uploader.start_bundle()
  uploader.process([], )
  uploader._get_analytics_service.assert_not_called()


def test_work_with_empty_elements(uploader, mocker, caplog):
  mocker.patch.object(uploader, '_get_analytics_service')
  uploader.process([], )
  uploader._get_analytics_service.assert_not_called()
  assert 'Skipping upload to GA, received no elements.' in caplog.text


def test_fail_having_more_than_one_execution(uploader):
  exec1 = Execution('orig1', SourceType.BIG_QUERY, ('dt1', 'buyers'), 'dest1', Action.GA_USER_LIST_UPLOAD,
                    ('a', 'b', 'c', 'd', 'e', 'f'))
  exec2 = Execution('origi2', SourceType.BIG_QUERY, ('dt2', 'buyers2'), 'dest2', Action.GA_USER_LIST_UPLOAD,
                    ('a', 'b', 'c', 'd', 'e', 'f'))

  with pytest.raises(ValueError, match='At least two Execution in a single call'):
    uploader.process([{'execution': exec1}, {'execution': exec2}], )


def test_fail_with_wrong_action(mocker, uploader):
  execution = Execution('orig1', SourceType.BIG_QUERY, ('dt1', 'buyers'), 'dest1', Action.ADS_SSD_UPLOAD,
                        ('a', 'b', 'c', 'd', 'e', 'f'))

  with pytest.raises(ValueError, match='Wrong Action received'):
    uploader.process([{'execution': execution}], )


def test_fail_missing_destination_metadata(uploader):
  execution = Execution('orig1', SourceType.BIG_QUERY, ('dt1', 'buyers'), 'dest1', Action.GA_USER_LIST_UPLOAD,
                        ('a', 'b'))
  with pytest.raises(ValueError, match='Missing destination information'):
    uploader.process([{'execution': execution}], )

  assert_empty_destination_metadata(uploader, ('a', '', 'c', 'd', 'e', 'f'))


def assert_empty_destination_metadata(uploader, destination_metadata):
  execution = Execution('orig1', SourceType.BIG_QUERY, ('dt1', 'buyers'), 'dest1', Action.GA_USER_LIST_UPLOAD,
                        destination_metadata)
  with pytest.raises(ValueError, match='Missing destination information'):
    uploader.process([{'execution': execution}], )


def test_list_already_exists(mocker, uploader):
  service = mocker.MagicMock()
  service.management().remarketingAudience().list().execute = mocker.Mock(
    return_value={'items': [{'id': 1, 'name': 'list'}]})

  mocker.patch.object(uploader, '_get_analytics_service')
  uploader._get_analytics_service.return_value = service

  execution = Execution('orig1', SourceType.BIG_QUERY, ('dt1', 'buyers'), 'dest1', Action.GA_USER_LIST_UPLOAD,
                        ('a', 'b', 'c', 'list', 'd', 'e'))
  uploader.process([{'execution': execution, 'row': ()}])

  uploader._get_analytics_service().management().remarketingAudience().insert.assert_not_called()


def test_list_creation(mocker, uploader):
  service = mocker.MagicMock()

  mocker.patch.object(uploader, '_get_analytics_service')
  uploader._get_analytics_service.return_value = service

  service.management().remarketingAudience().insert().execute.return_value = {'id': 1}

  execution = Execution('orig1', SourceType.BIG_QUERY, ('dt1', 'buyers'), 'dest1', Action.GA_USER_LIST_UPLOAD,
                        ('web_property', 'view', 'c', 'list', 'd', 'buyers_custom_dim'))
  uploader.process([{'execution': execution, 'row': ()}])

  service.management().remarketingAudience().insert.assert_any_call(
    accountId="acc",
    webPropertyId='web_property',
    body={
      'name': 'list',
      'linkedViews': ['view'],
      'linkedAdAccounts': [{
        'type': 'ADWORDS_LINKS',
        'linkedAccountId': 'xxx-yyy-zzzz'
      }],
      'audienceType': 'SIMPLE',
      'audienceDefinition': {
        'includeConditions': {
          'kind': 'analytics#includeConditions',
          'isSmartList': False,
          'segment': 'users::condition::%s==buyer' % 'buyers_custom_dim',
          'membershipDurationDays': 365
        }
      }
    }
  )


def test_elements_uploading(mocker, uploader):
  service = mocker.MagicMock()

  mocker.patch.object(uploader, '_get_analytics_service')
  uploader._get_analytics_service.return_value = service

  service.management().customDataSources().list().execute.return_value = {
    'items': [{'id': 1, 'name': 'data_import_name'}]}

  execution = Execution('orig1', SourceType.BIG_QUERY, ('dt1', 'buyers'), 'dest1', Action.GA_USER_LIST_UPLOAD,
                        ('web_property', 'b', 'data_import_name', 'd', 'user_id_custom_dim', 'buyer_custom_dim'))
  uploader.process([{'execution': execution, 'row': {'user_id': '12'}},
                    {'execution': execution, 'row': {'user_id': '34'}}])

  body = 'user_id_custom_dim, buyer_custom_dim\n' \
         '12,buyer\n' \
         '34,buyer'

  media = MediaInMemoryUpload(bytes(body, 'UTF-8'),
                              mimetype='application/octet-stream',
                              resumable=True)
  # service.management().uploads().uploadData.assert_any_call(
  #   accountId="acc",
  #   webPropertyId='web_property',
  #   customDataSourceId=1,
  #   media_body=media
  # )
