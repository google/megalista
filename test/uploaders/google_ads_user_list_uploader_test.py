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

from megalist_dataflow.uploaders.google_ads_user_list_uploader import GoogleAdsUserListUploaderDoFn
from megalist_dataflow.utils.oauth_credentials import OAuthCredentials
from apache_beam.options.value_provider import StaticValueProvider
from googleads import adwords
from googleads import oauth2
import pytest


@pytest.fixture
def uploader(mocker):
    mocker.patch('googleads.oauth2.GoogleRefreshTokenClient')
    mocker.patch('googleads.adwords.AdWordsClient')
    id = StaticValueProvider(str, "id")
    secret = StaticValueProvider(str, "secret")
    access = StaticValueProvider(str, "access")
    refresh = StaticValueProvider(str, "refresh")
    credentials = OAuthCredentials(id, secret, access, refresh)
    return GoogleAdsUserListUploaderDoFn(credentials, StaticValueProvider(str,"devtoken"), StaticValueProvider(str,"123-456-7890"))


def test_get_service(mocker, uploader):
    assert uploader._get_user_list_service() != None

def test_not_active(mocker):
    id = StaticValueProvider(str, "id")
    secret = StaticValueProvider(str, "secret")
    access = StaticValueProvider(str, "access")
    refresh = StaticValueProvider(str, "refresh")
    credentials = OAuthCredentials(id, secret, access, refresh)
    uploader = GoogleAdsUserListUploaderDoFn(credentials, None, "123-456-7890")
    mocker.patch.object(uploader, '_get_user_list_service')
    uploader.start_bundle()
    uploader.process([])
    uploader._get_user_list_service.assert_not_called()


def test_list_created(mocker, uploader):
    result = mocker.MagicMock()
    result.entries = [mocker.MagicMock()]
    mocker.patch.object(uploader, '_get_user_list_service')
    uploader._get_user_list_service().get = mocker.Mock(return_value=result)
    uploader.start_bundle()
    uploader._get_user_list_service().mutate.assert_not_called()


def test_list_creation(mocker, uploader):
    mocker.patch.object(uploader, '_get_user_list_service')
    uploader.start_bundle()
    uploader._get_user_list_service().mutate.assert_any_call([{'operator': 'ADD', 'operand': {
        'xsi_type': 'CrmBasedUserList', 'name': 'Megalist - CRM - Buyers', 'description': 'Megalist - CRM - Buyers', 'membershipLifeSpan': 10000, 'uploadKeyType': 'CONTACT_INFO'}}])
    uploader._get_user_list_service().mutate.assert_any_call([{'operator': 'ADD', 'operand': {'xsi_type': 'LogicalUserList', 'name': 'Megalist - CRM - Potential New Buyers',
                                                                                              'description': 'Megalist - CRM - Potential New Buyers', 'status': 'OPEN', 'rules': [{'operator': 'NONE', 'ruleOperands': [{'UserList': {'id': mocker.ANY, 'xsi_type': 'CrmBasedUserList'}}]}]}}])


def test_element_uploading(mocker, uploader):
    mocker.patch.object(uploader, '_get_user_list_service')
    uploader.user_list_id = 123
    uploader.process(['a', 'b'])
    uploader._get_user_list_service().mutateMembers.assert_any_call(
        [{'operand': {'membersList': ['a', 'b'], 'userListId': 123}, 'operator': 'ADD'}])
