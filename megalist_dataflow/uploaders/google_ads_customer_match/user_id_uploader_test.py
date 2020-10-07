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

import unittest

from apache_beam.options.value_provider import StaticValueProvider
from uploaders.google_ads_customer_match import user_id_uploader
from utils.oauth_credentials import OAuthCredentials
import pytest



@pytest.fixture
def uploader(mocker):
  mocker.patch('googleads.oauth2.GoogleRefreshTokenClient')
  mocker.patch('googleads.adwords.AdWordsClient')

  client_id = StaticValueProvider(str, 'id')
  secret = StaticValueProvider(str, 'secret')
  access = StaticValueProvider(str, 'access')
  refresh = StaticValueProvider(str, 'refresh')
  dev_token = StaticValueProvider(str, 'dev_token')
  credentials = OAuthCredentials(client_id, secret, access, refresh)

  return user_id_uploader.GoogleAdsCustomerMatchUserIdUploaderDoFn(
      credentials, dev_token)


def test_list_creation(uploader):
  uploader.process([],)
