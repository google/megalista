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

from megalist_dataflow.uploaders.google_analytics_user_list_uploader import GoogleAnalyticsUserListUploaderDoFn
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
    account_id = StaticValueProvider(str, "acc")
    web_property_id = StaticValueProvider(str, "1234")
    google_ads_account = StaticValueProvider(str, "xxx-yyy-zzzz")
    user_id_custom_dim = StaticValueProvider(str, "cd1")
    buyer_custom_dim = StaticValueProvider(str, "cd2")
    return GoogleAnalyticsUserListUploaderDoFn(credentials, account_id, web_property_id, google_ads_account, user_id_custom_dim, buyer_custom_dim)


def test_get_service(mocker, uploader):
    assert uploader._get_analytics_service() != None

def test_not_active(mocker):
    id = StaticValueProvider(str, "id")
    secret = StaticValueProvider(str, "secret")
    access = StaticValueProvider(str, "access")
    refresh = StaticValueProvider(str, "refresh")
    credentials = OAuthCredentials(id, secret, access, refresh)
    uploader = GoogleAnalyticsUserListUploaderDoFn(credentials, None, None, None, None, None)
    mocker.patch.object(uploader, '_get_analytics_service')
    uploader.start_bundle()
    uploader.process([])
    uploader._get_analytics_service.assert_not_called()


# def test_list_created(mocker, uploader):
#     result = mocker.MagicMock()
#     result.entries = [mocker.MagicMock()]
#     mocker.patch.object(uploader, '_get_analytics_service')
#     uploader._get_analytics_service().management = mocker.Mock(return_value=result)
#     lists = {'items':[{'name': 'Megalist - GA - Buyers', 'id': '555'}, {'name': 'Megalist - GA - Buyers', 'id': '555'}]}
#     uploader._get_analytics_service().management().remarketingAudience().list().execute= mocker.Mock(return_value=lists)
#     uploader.start_bundle()
#     uploader._get_analytics_service().management().remarketingAudience().insert.assert_not_called()
#     assert False


# def test_list_creation(mocker, uploader):
#     mocker.patch.object(uploader, '_get_user_list_service')
#     uploader.start_bundle()
#     uploader._get_user_list_service().mutate.assert_any_call([{'operator': 'ADD', 'operand': {
#         'xsi_type': 'CrmBasedUserList', 'name': 'Megalist - CRM - Buyers', 'description': 'Megalist - CRM - Buyers', 'membershipLifeSpan': 10000, 'uploadKeyType': 'CONTACT_INFO'}}])
#     uploader._get_user_list_service().mutate.assert_any_call([{'operator': 'ADD', 'operand': {'xsi_type': 'LogicalUserList', 'name': 'Megalist - CRM - Potential New Buyers',
#                                                                                               'description': 'Megalist - CRM - Potential New Buyers', 'status': 'OPEN', 'rules': [{'operator': 'NONE', 'ruleOperands': [{'UserList': {'id': mocker.ANY, 'xsi_type': 'CrmBasedUserList'}}]}]}}])


# def test_element_uploading(mocker, uploader):
#     mocker.patch.object(uploader, '_get_user_list_service')
#     uploader.user_list_id = 123
#     uploader.process(['a', 'b'])
#     uploader._get_user_list_service().mutateMembers.assert_any_call(
#         [{'operand': {'membersList': ['a', 'b'], 'userListId': 123}, 'operator': 'ADD'}])
