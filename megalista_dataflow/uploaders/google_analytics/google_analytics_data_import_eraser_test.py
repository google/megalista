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

from error.error_handling import ErrorHandler
from error.error_handling_test import MockErrorNotifier
from models.execution import Execution, SourceType, DestinationType, Source, AccountConfig, Destination, Batch
from models.oauth_credentials import OAuthCredentials
from uploaders.google_analytics.google_analytics_data_import_eraser import GoogleAnalyticsDataImportEraser


@pytest.fixture
def error_notifier():
    return MockErrorNotifier()

@pytest.fixture
def eraser(error_notifier):
    client_id = StaticValueProvider(str, "id")
    secret = StaticValueProvider(str, "secret")
    access = StaticValueProvider(str, "access")
    refresh = StaticValueProvider(str, "refresh")
    credentials = OAuthCredentials(client_id, secret, access, refresh)
    return GoogleAnalyticsDataImportEraser(credentials,
                                           ErrorHandler(DestinationType.GA_DATA_IMPORT, error_notifier))


def test_analytics_has_not_data_sources(mocker, eraser, caplog, error_notifier):
    service = mocker.MagicMock()

    mocker.patch.object(eraser, '_get_analytics_service')
    eraser._get_analytics_service.return_value = service

    service.management().customDataSources().list().execute.return_value = {
        'items': []
    }

    execution = Execution(AccountConfig('', False, '', '', ''),
                          Source('orig1', SourceType.BIG_QUERY, ['dt1', 'buyers']),
                          Destination('dest1', DestinationType.GA_DATA_IMPORT, ['web_property', 'data_import_name']))
    # Act
    eraser.process(Batch(execution, []))

    eraser.finish_bundle()

    assert 'data_import_name - data import not found, please configure it in Google Analytics' in caplog.text

    assert error_notifier.were_errors_sent


def test_data_source_not_found(mocker, eraser, caplog, error_notifier):
    service = mocker.MagicMock()

    mocker.patch.object(eraser, '_get_analytics_service')
    eraser._get_analytics_service.return_value = service

    service.management().customDataSources().list().execute.return_value = {
        'items': [{'id': 1, 'name': 'wrong_name'}]
    }

    execution = Execution(AccountConfig('', False, '', '', ''),
                          Source('orig1', SourceType.BIG_QUERY, ['dt1', 'buyers']),
                          Destination('dest1', DestinationType.GA_DATA_IMPORT, ['web_property', 'data_import_name']))

    eraser.process(Batch(execution, []))

    # Act
    eraser.finish_bundle()

    assert 'data_import_name - data import not found, please configure it in Google Analytics' in caplog.text

    assert error_notifier.were_errors_sent

def test_no_files_found(mocker, eraser):
    service = mocker.MagicMock()

    mocker.patch.object(eraser, '_get_analytics_service')
    eraser._get_analytics_service.return_value = service

    service.management().customDataSources().list().execute.return_value = {
        'items': [{'id': 1, 'name': 'data_import_name'},
                  {'id': 2, 'name': 'data_import_name2'}]
    }

    execution = Execution(AccountConfig('', False, '', '', ''),
                          Source('orig1', SourceType.BIG_QUERY, ['dt1', 'buyers']),
                          Destination('dest1', DestinationType.GA_DATA_IMPORT, ['web_property', 'data_import_name']))

    # Add mock to side effect of list uploads
    service.management().uploads().list().execute.return_value = {'items': []}

    # Add mock to side effect of deleteUploadData
    delete_call_mock = mocker.MagicMock()
    service.management().uploads().deleteUploadData.side_effect = delete_call_mock

    # Act
    eraser.process(Batch(execution, []))

    # Called once
    delete_call_mock.assert_not_called()


def test_files_deleted_with_success(mocker, eraser):
    service = mocker.MagicMock()

    mocker.patch.object(eraser, '_get_analytics_service')
    eraser._get_analytics_service.return_value = service

    service.management().customDataSources().list().execute.return_value = {
        'items': [{'id': 1, 'name': 'data_import_name'},
                  {'id': 2, 'name': 'data_import_name2'}]
    }

    execution = Execution(AccountConfig('', False, '', '', ''),
                          Source('orig1', SourceType.BIG_QUERY, ['dt1', 'buyers']),
                          Destination('dest1', DestinationType.GA_DATA_IMPORT, ['web_property', 'data_import_name']))

    # Add mock to side effect of list uploads
    service.management().uploads().list().execute.return_value = {'items': [{'id': 'ab'}, {'id': 'cd'}]}

    # Add mock to side effect of deleteUploadData
    delete_call_mock = mocker.MagicMock()
    service.management().uploads().deleteUploadData.side_effect = delete_call_mock

    # Act
    eraser.process(Batch(execution, []))

    # Called once
    delete_call_mock.assert_called_once()

    # Intercept args called
    _, kwargs = delete_call_mock.call_args

    # Check if really sent values from custom field
    _ = kwargs['body']

    # assert
