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

from megalist_dataflow.uploaders.google_ads_user_list_uploader import GoogleAdsUserListUploaderDoFn
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
  return GoogleAdsUserListUploaderDoFn(credentials, StaticValueProvider(str, "devtoken"),
                                       StaticValueProvider(str, "123-456-7890"), StaticValueProvider(str, "com.app.id"))


def test_get_service(mocker, uploader):
  assert uploader._get_user_list_service() is not None


def test_not_active(mocker, caplog):
  id = StaticValueProvider(str, "id")
  secret = StaticValueProvider(str, "secret")
  access = StaticValueProvider(str, "access")
  refresh = StaticValueProvider(str, "refresh")
  credentials = OAuthCredentials(id, secret, access, refresh)
  uploader = GoogleAdsUserListUploaderDoFn(credentials, None, "123-456-7890", "com.app.id")
  mocker.patch.object(uploader, '_get_user_list_service')
  uploader.process([], )
  uploader._get_user_list_service.assert_not_called()
  assert 'Skipping upload to ads, parameters not configured.' in caplog.text


def test_work_with_empty_elements(mocker, caplog):
  id = StaticValueProvider(str, "id")
  secret = StaticValueProvider(str, "secret")
  access = StaticValueProvider(str, "access")
  refresh = StaticValueProvider(str, "refresh")
  credentials = OAuthCredentials(id, secret, access, refresh)
  uploader = GoogleAdsUserListUploaderDoFn(credentials, "123", "123-456-7890", "com.app.id")
  mocker.patch.object(uploader, '_get_user_list_service')
  uploader.process([], )
  uploader._get_user_list_service.assert_not_called()
  assert 'Skipping upload to ads, received no elements.' in caplog.text


def test_fail_having_more_than_one_execution(mocker):
  id = StaticValueProvider(str, "id")
  secret = StaticValueProvider(str, "secret")
  access = StaticValueProvider(str, "access")
  refresh = StaticValueProvider(str, "refresh")
  credentials = OAuthCredentials(id, secret, access, refresh)
  uploader = GoogleAdsUserListUploaderDoFn(credentials, "123", "123-456-7890", "com.app.id")
  mocker.patch.object(uploader, '_get_user_list_service')

  exec1 = Execution('orig1', SourceType.BIG_QUERY, ('dt1', 'buyers'), Action.ADS_USER_LIST_UPLOAD, 'dest1',
                    ('a', 'b', 'c'))
  exec2 = Execution('origi2', SourceType.BIG_QUERY, ('dt2', 'buyers2'), Action.ADS_USER_LIST_UPLOAD, 'dest2',
                    ('a', 'b', 'c'))

  with pytest.raises(ValueError, match='At least two Execution in a single call'):
    uploader.process([{'execution': exec1}, {'execution': exec2}])


def test_list_already_created(mocker, uploader):
  result = mocker.MagicMock()
  result.entries = [mocker.MagicMock()]
  user_list_service = mocker.MagicMock()
  user_list_service.get = mocker.Mock(return_value=result)
  uploader._create_list_if_it_does_not_exist(user_list_service, '', '')
  uploader._get_user_list_service().mutate.assert_not_called()


def test_lists_creation(mocker, uploader):
  mocker.patch.object(uploader, '_get_user_list_service')

  uploader._create_lists('crm', 'mobile', 'rev')
  assert_crm_list_creation(uploader._get_user_list_service(), 'crm')
  assert_mobile_list_creation(uploader._get_user_list_service(), 'mobile')
  assert_logical_list_creation(mocker, uploader._get_user_list_service(), 'rev')


def assert_crm_list_creation(user_list_service, list_name):
  user_list_service.mutate.assert_any_call([{
    'operator': 'ADD',
    'operand': {
      'xsi_type': 'CrmBasedUserList',
      'name': list_name,
      'description': list_name,
      'membershipLifeSpan': 10000,
      'uploadKeyType': 'CONTACT_INFO'
    }
  }])


def assert_mobile_list_creation(user_list_service, list_name):
  user_list_service.mutate.assert_any_call([{
    'operator': 'ADD',
    'operand': {
      'xsi_type': 'CrmBasedUserList',
      'name': list_name,
      'description': list_name,
      # CRM-based user lists can use a membershipLifeSpan of 10000 to indicate
      # unlimited; otherwise normal values apply.
      'membershipLifeSpan': 10000,
      'appId': 'com.app.id',
      'uploadKeyType': 'MOBILE_ADVERTISING_ID'
    }
  }])


def assert_logical_list_creation(mocker, user_list_service, list_name):
  user_list_service.mutate.assert_any_call([{
    'operator': 'ADD',
    'operand': {
      'xsi_type': 'LogicalUserList',
      'name': list_name,
      'description': list_name,
      'status': 'OPEN',
      'rules': [{
        'operator': 'NONE',
        'ruleOperands': [
          {'UserList': {'id': mocker.ANY, 'xsi_type': 'CrmBasedUserList'}},
          {'UserList': {'id': mocker.ANY, 'xsi_type': 'CrmBasedUserList'}}]}]}}])


def test_fail_missing_destination_metadata(uploader):
  execution = Execution('orig1', SourceType.BIG_QUERY, ('dt1', 'buyers'), Action.ADS_USER_LIST_UPLOAD, 'dest1',
                        ('a', 'b'))
  with pytest.raises(ValueError, match='Missing destination information'):
    uploader.process([{'execution': execution}])


def test_fail_empty_first_metadata(uploader):
  assert_empty_destination_metadata(uploader, (None, 'b', 'c'))
  assert_empty_destination_metadata(uploader, ('', 'b', 'c'))

  assert_empty_destination_metadata(uploader, ('a', None, 'c'))
  assert_empty_destination_metadata(uploader, ('a', '', 'c'))

  assert_empty_destination_metadata(uploader, ('a', 'b', None))
  assert_empty_destination_metadata(uploader, ('a', 'b', ''))


def assert_empty_destination_metadata(uploader, destination_metadata):
  execution = Execution('orig1', SourceType.BIG_QUERY, ('dt1', 'buyers'), Action.ADS_USER_LIST_UPLOAD, 'dest1',
                        destination_metadata)
  with pytest.raises(ValueError, match='Missing destination information'):
    uploader.process([{'execution': execution}])


def test_element_uploading(mocker, uploader):
  user_list = 'user_list'
  mobile_list = 'mobile_list'

  mocker.patch.object(uploader, '_create_lists')
  uploader._create_lists.return_value = (user_list, mobile_list)


  execution = Execution('orig1', SourceType.BIG_QUERY, ('dt1', 'buyers'), Action.ADS_USER_LIST_UPLOAD, 'dest1',
                        (user_list, mobile_list, 'c'))
  uploader.process([{'execution': execution, "row": {'mobileId': 'a', 'email': 'x@x.com'}},
                    {'execution': execution, 'row': {'mobileId': 'b', 'email': 'y@y.com'}}])
  uploader._get_user_list_service().mutateMembers.assert_any_call(
    [{'operand': {
      'membersList': [
        {'mobileId': 'a'},
        {'mobileId': 'b'}],
      'userListId': mobile_list},
      'operator': 'ADD'}])
  uploader._get_user_list_service().mutateMembers.assert_any_call(
    [{'operand': {
      'membersList': [
        {'email': 'x@x.com'},
        {'email': 'y@y.com'}],
      'userListId': user_list},
      'operator': 'ADD'}])
