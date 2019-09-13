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

from megalist_dataflow.utils.oauth_credentials import OAuthCredentials
from apache_beam.options.value_provider import StaticValueProvider


def test_init():
    id = StaticValueProvider(str, "id")
    secret = StaticValueProvider(str, "secret")
    access = StaticValueProvider(str, "access")
    refresh = StaticValueProvider(str, "refresh")
    credentials = OAuthCredentials(id, secret, access, refresh)
    assert credentials.get_client_id() == "id"
    assert credentials.get_client_secret() == "secret"
    assert credentials.get_access_token() == "access"
    assert credentials.get_refresh_token() == "refresh"
