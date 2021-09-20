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

import pytest
from apache_beam.options.value_provider import StaticValueProvider

from models.oauth_credentials import OAuthCredentials
from uploaders.google_analytics.google_analytics_data_import_uploader import GoogleAnalyticsDataImportUploaderDoFn
from models.execution import AccountConfig
from models.execution import Destination
from models.execution import DestinationType
from models.execution import Execution
from models.execution import Source
from models.execution import SourceType
from models.execution import Batch

_account_config = AccountConfig('1234567890', False, '1234567890', '', '')


@pytest.fixture
def uploader(mocker):
  client_id = StaticValueProvider(str, 'id')
  secret = StaticValueProvider(str, 'secret')
  access = StaticValueProvider(str, 'access')
  refresh = StaticValueProvider(str, 'refresh')
  credentials = OAuthCredentials(client_id, secret, access, refresh)
  return GoogleAnalyticsDataImportUploaderDoFn(credentials)


def test_get_service(uploader):
  assert uploader._get_analytics_service() is not None


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
      _account_config,
      Source('orig1', SourceType.BIG_QUERY, ['dt1', 'buyers']),
      Destination('dest1', DestinationType.GA_DATA_IMPORT,
                  ['web_property', 'data_import_name']),
                  Execution.ExecutionConfigurationMedium.JSON)

  # Add mock to side effect of uploadData()
  my_mock = mocker.MagicMock()
  service.management().uploads().uploadData.side_effect = my_mock

  # Act
  uploader.process(Batch(execution, [{
          'user_id': '12',
          'cd1': 'value1a',
          'cd2': 'value2a'
      },
      {
          'user_id': '34',
          'cd1': 'value1b',
          'cd2': 'value2b'
      },
      {
          'user_id': '56',
          'cd1': None,
          'cd2': ''
      }]))
  

  # Called once
  my_mock.assert_called_once()

  # Intercept args called
  _, kwargs = my_mock.call_args

  # Check if really sent values from custom field
  media_bytes = kwargs['media_body'].getbytes(0, -1)

  print(media_bytes)
  assert media_bytes == b'ga:user_id,ga:cd1,ga:cd2\n' \
                        b'12,value1a,value2a\n' \
                        b'34,value1b,value2b\n' \
                        b'56,,'
