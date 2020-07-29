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

from megalist_dataflow.utils.oauth_credentials import OAuthCredentials
from uploaders.google_analytics_data_import_uploader import GoogleAnalyticsDataImportUploaderDoFn
from utils.execution import Execution, SourceType, DestinationType, Source, AccountConfig, Destination


@pytest.fixture
def uploader(mocker):
    mocker.patch('googleads.oauth2.GoogleRefreshTokenClient')
    mocker.patch('googleads.adwords.AdWordsClient')
    client_id = StaticValueProvider(str, "id")
    secret = StaticValueProvider(str, "secret")
    access = StaticValueProvider(str, "access")
    refresh = StaticValueProvider(str, "refresh")
    credentials = OAuthCredentials(client_id, secret, access, refresh)
    return GoogleAnalyticsDataImportUploaderDoFn(credentials)


def test_get_service(uploader):
    assert uploader._get_analytics_service() is not None


def test_work_with_empty_elements(uploader, mocker, caplog):
    mocker.patch.object(uploader, '_get_analytics_service')
    uploader.process([], )
    uploader._get_analytics_service.assert_not_called()
    assert 'Skipping upload, received no elements.' in caplog.text


def test_fail_having_more_than_one_execution(uploader, caplog):
    exec1 = Execution(AccountConfig('', False, '', '', ''),
                      Source('orig1', SourceType.BIG_QUERY, ['dt1', 'buyers']),
                      Destination('dest1', DestinationType.GA_DATA_IMPORT, ['a', 'b', 'c', 'd', 'e', 'f']))
    exec2 = Execution(AccountConfig('', False, '', '', ''),
                      Source('orig2', SourceType.BIG_QUERY, ['dt2', 'buyers2']),
                      Destination('dest2', DestinationType.GA_DATA_IMPORT, ['a', 'b', 'c', 'd', 'e', 'f']))

    uploader.process([{'execution': exec1, 'row': ()}, {'execution': exec2, 'row': ()}], )
    assert 'At least two Execution in a single call' in caplog.text


def test_fail_with_wrong_action(uploader, caplog):
    execution = Execution(AccountConfig('', False, '', '', ''),
                          Source('orig1', SourceType.BIG_QUERY, ['dt1', 'buyers']),
                          Destination('dest1', DestinationType.ADS_SSD_UPLOAD, ['a', 'b', 'c', 'd', 'e', 'f']))

    uploader.process([{'execution': execution, 'row': ()}], )
    assert 'Wrong Action received' in caplog.text


def test_fail_missing_destination_metadata(uploader, caplog):
    execution = Execution(AccountConfig('', False, '', '', ''),
                          Source('orig1', SourceType.BIG_QUERY, ['dt1', 'buyers']),
                          Destination('dest1', DestinationType.GA_DATA_IMPORT, ['a']))

    uploader.process([{'execution': execution, 'row': ()}], )
    assert 'Missing destination information' in caplog.text

    assert_empty_destination_metadata(uploader, ('a', ''), caplog)


def assert_empty_destination_metadata(uploader, destination_metadata, caplog):
    execution = Execution(AccountConfig('', False, '', '', ''),
                          Source('orig1', SourceType.BIG_QUERY, ['dt1', 'buyers']),
                          Destination('dest1', DestinationType.GA_DATA_IMPORT, destination_metadata))
    uploader.process([{'execution': execution, 'row': ()}], )
    assert 'Missing destination information' in caplog.text


def test_elements_uploading(mocker, uploader):
    service = mocker.MagicMock()

    mocker.patch.object(uploader, '_get_analytics_service')
    uploader._get_analytics_service.return_value = service

    service.management().customDataSources().list().execute.return_value = {
        'items': [{'id': 1, 'name': 'data_import_name'}]
    }

    execution = Execution(AccountConfig('', False, '', '', ''),
                          Source('orig1', SourceType.BIG_QUERY, ['dt1', 'buyers']),
                          Destination('dest1', DestinationType.GA_DATA_IMPORT, ['web_property', 'data_import_name']))

    # Add mock to side effect of uploadData()
    my_mock = mocker.MagicMock()
    service.management().uploads().uploadData.side_effect = my_mock

    # Act
    uploader.process([{'execution': execution, 'row': {'user_id': '12', 'cd1': 'value1a', 'cd2': 'value2a'}},
                      {'execution': execution, 'row': {'user_id': '34', 'cd1': 'value1b', 'cd2': 'value2b'}},
                      {'execution': execution, 'row': {'user_id': '56', 'cd1': None, 'cd2': ''}}])

    # Called once
    my_mock.assert_called_once()

    # Intercept args called
    _, kwargs = my_mock.call_args

    # Check if really sent values from custom field
    media_bytes = kwargs['media_body'].getbytes(0, -1)

    assert media_bytes == b'user_id,cd1,cd2\n' \
                          b'12,value1a,value2a\n' \
                          b'34,value1b,value2b\n' \
                          b'56,,'
