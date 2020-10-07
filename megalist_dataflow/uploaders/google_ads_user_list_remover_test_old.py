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

from utils.oauth_credentials import OAuthCredentials
from apache_beam.options.value_provider import StaticValueProvider
import pytest

from uploaders.google_ads_user_list_remover import GoogleAdsUserListRemoverDoFn
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
  return GoogleAdsUserListRemoverDoFn(credentials, StaticValueProvider(str, "devtoken"),
                                      StaticValueProvider(str, "123-456-7890"), StaticValueProvider(str, "com.app.id"))


def test_get_service(mocker, uploader):
  assert uploader._get_user_list_service() is not None


def test_not_active(mocker, caplog):
  id = StaticValueProvider(str, "id")
  secret = StaticValueProvider(str, "secret")
  access = StaticValueProvider(str, "access")
  refresh = StaticValueProvider(str, "refresh")
  credentials = OAuthCredentials(id, secret, access, refresh)
  uploader = GoogleAdsUserListRemoverDoFn(credentials, None, "123-456-7890", "com.app.id")
  mocker.patch.object(uploader, '_get_user_list_service')
  uploader.process([], )
  uploader._get_user_list_service.assert_not_called()
  assert 'Skipping upload to ads, parameters not configured.' in caplog.text


def test_work_with_empty_elements(uploader, mocker, caplog):
  mocker.patch.object(uploader, '_get_user_list_service')
  uploader.process([], )
  uploader._get_user_list_service.assert_not_called()
  assert 'Skipping upload to ads, received no elements.' in caplog.text


def test_fail_having_more_than_one_execution(uploader):
  exec1 = Execution('orig1', SourceType.BIG_QUERY, ('dt1', 'buyers'), 'dest1', Action.ADS_USER_LIST_REMOVE,
                    ('a', 'b'))
  exec2 = Execution('origi2', SourceType.BIG_QUERY, ('dt2', 'buyers2'), 'dest2', Action.ADS_USER_LIST_REMOVE,
                    ('a', 'b'))

  with pytest.raises(ValueError, match='At least two Execution in a single call'):
    uploader.process([{'execution': exec1}, {'execution': exec2}], )


def test_fail_with_wrong_action(mocker, uploader):
  execution = Execution('origi2', SourceType.BIG_QUERY, ('dt2', 'buyers2'), 'dest2', Action.ADS_USER_LIST_UPLOAD,
                        ('a', 'b'))

  with pytest.raises(ValueError, match='Wrong Action received'):
    uploader.process([{'execution': execution}], )


def test_fail_missing_destination_metadata(uploader):
  execution = Execution('orig1', SourceType.BIG_QUERY, ('dt1', 'buyers'), 'dest1', Action.ADS_USER_LIST_REMOVE, ('a'))
  with pytest.raises(ValueError, match='Missing destination information'):
    uploader.process([{'execution': execution}], )


def test_fail_empty_first_metadata(uploader):
  assert_empty_destination_metadata(uploader, (None, 'b'))
  assert_empty_destination_metadata(uploader, ('', 'b'))

  assert_empty_destination_metadata(uploader, ('a', None))
  assert_empty_destination_metadata(uploader, ('a', ''))


def assert_empty_destination_metadata(uploader, destination_metadata):
  execution = Execution('orig1', SourceType.BIG_QUERY, ('dt1', 'buyers'), 'dest1', Action.ADS_USER_LIST_REMOVE,
                        destination_metadata)
  with pytest.raises(ValueError, match='Missing destination information'):
    uploader.process([{'execution': execution}], )


def test_element_removing(mocker, uploader):
  user_list = 'user_list'

  mocker.patch.object(uploader, '_do_get_list_id')
  uploader._do_get_list_id.return_value = user_list

  execution = Execution('orig1', SourceType.BIG_QUERY, ('dt1', 'buyers'), 'dest1', Action.ADS_USER_LIST_REMOVE,
                        (user_list, user_list))
  uploader.process([{'execution': execution, "row": {'mobileId': 'a', 'hashedEmail': 'x@x.com'}},
                    {'execution': execution, 'row': {'mobileId': 'b', 'hashedEmail': 'y@y.com'}}], )
  uploader._get_user_list_service().mutateMembers.assert_any_call(
    [{'operand': {
      'membersList': [
        {'mobileId': 'a'},
        {'mobileId': 'b'}],
      'userListId': user_list},
      'operator': 'REMOVE'}])
  uploader._get_user_list_service().mutateMembers.assert_any_call(
    [{'operand': {
      'membersList': [
        {'hashedEmail': 'x@x.com'},
        {'hashedEmail': 'y@y.com'}],
      'userListId': user_list},
      'operator': 'REMOVE'}])
