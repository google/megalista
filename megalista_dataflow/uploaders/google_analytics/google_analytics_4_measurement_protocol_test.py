# Copyright 2021 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import pytest
import requests_mock

from error.error_handling import ErrorHandler
from error.error_handling_test import MockErrorNotifier
from models.execution import Execution, SourceType, DestinationType, Source, AccountConfig, Destination, Batch
from uploaders.google_analytics.google_analytics_4_measurement_protocol import \
    GoogleAnalytics4MeasurementProtocolUploaderDoFn

_account_config = AccountConfig('account_id', False, 'ga_account_id', '', '')

@pytest.fixture
def error_notifier():
    return MockErrorNotifier()

@pytest.fixture
def uploader(error_notifier):
    return GoogleAnalytics4MeasurementProtocolUploaderDoFn(
        ErrorHandler(DestinationType.GA_4_MEASUREMENT_PROTOCOL, error_notifier))


def test_exception_event_and_user_property(uploader):
    with requests_mock.Mocker() as m:
        m.post(requests_mock.ANY, status_code=204)
        destination = Destination(
            'dest1', DestinationType.GA_4_MEASUREMENT_PROTOCOL, [
                'api_secret',
                'True',
                'True',
                '',
                'some_id',
                ''
            ])
        source = Source('orig1', SourceType.BIG_QUERY, ['', ''])
        execution = Execution(_account_config, source, destination)
        with pytest.raises(ValueError, match='GA4 MP should be called either for sending events'):
            uploader.do_process(Batch(execution, []))


def test_exception_no_event_nor_user_property(uploader):
    with requests_mock.Mocker() as m:
        m.post(requests_mock.ANY, status_code=204)
        destination = Destination(
            'dest1', DestinationType.GA_4_MEASUREMENT_PROTOCOL, [
                'api_secret',
                'False',
                'False',
                '',
                'some_id',
                ''
            ])
        source = Source('orig1', SourceType.BIG_QUERY, ['', ''])
        execution = Execution(_account_config, source, destination)
        with pytest.raises(ValueError, match='GA4 MP should be called either for sending events'):
            uploader.do_process(Batch(execution, []))


def test_exception_app_and_web(uploader):
    with requests_mock.Mocker() as m:
        m.post(requests_mock.ANY, status_code=204)
        destination = Destination(
            'dest1', DestinationType.GA_4_MEASUREMENT_PROTOCOL, [
                'api_secret',
                'False',
                'True',
                '',
                'some_app_id',
                'some_web_id'
            ])
        source = Source('orig1', SourceType.BIG_QUERY, ['', ''])
        execution = Execution(_account_config, source, destination)
        with pytest.raises(ValueError, match='GA4 MP should be called either with a firebase_app_id'):
            uploader.do_process(Batch(execution, [{
                'name': 'event_name',
            }]))


def test_exception_no_id(uploader):
    with requests_mock.Mocker() as m:
        m.post(requests_mock.ANY, status_code=204)
        destination = Destination(
            'dest1', DestinationType.GA_4_MEASUREMENT_PROTOCOL, [
                'api_secret',
                'False',
                'True',
                '',
                '',
                ''
            ])
        source = Source('orig1', SourceType.BIG_QUERY, ['', ''])
        execution = Execution(_account_config, source, destination)
        with pytest.raises(ValueError, match='GA4 MP should be called either with a firebase_app_id'):
            uploader.do_process(Batch(execution, [{
                'name': 'event_name',
                'value': '123'
            }]))

def test_exception_app_event_without_app_instance_id(uploader):
    with requests_mock.Mocker() as m:
        m.post(requests_mock.ANY, status_code=204)
        destination = Destination(
            'dest1', DestinationType.GA_4_MEASUREMENT_PROTOCOL, [
                'api_secret',
                'True',
                'False',
                '',
                'some_id',
                ''
            ])
        source = Source('orig1', SourceType.BIG_QUERY, ['', ''])
        execution = Execution(_account_config, source, destination)
        with pytest.raises(ValueError, match='GA4 MP needs an app_instance_id parameter when used for an App Stream.'):
            uploader.do_process(Batch(execution, [{
                'client_id': '123',
                'name': 'event_name',
                'value': '42',
                'important_event': 'False'
            }]))

def test_exception_web_event_without_client_id(uploader):
    with requests_mock.Mocker() as m:
        m.post(requests_mock.ANY, status_code=204)
        destination = Destination(
            'dest1', DestinationType.GA_4_MEASUREMENT_PROTOCOL, [
                'api_secret',
                'True',
                'False',
                '',
                '',
                'some_id'
            ])
        source = Source('orig1', SourceType.BIG_QUERY, ['', ''])
        execution = Execution(_account_config, source, destination)
        with pytest.raises(ValueError, match='GA4 MP needs a client_id parameter when used for a Web Stream.'):
            uploader.do_process(Batch(execution, [{
                'app_instance_id': '123',
                'name': 'event_name',
                'value': '42',
                'important_event': 'False'
            }]))

def test_succesful_app_event_call(uploader):
    with requests_mock.Mocker() as m:
        m.post(requests_mock.ANY, status_code=204)
        destination = Destination(
            'dest1', DestinationType.GA_4_MEASUREMENT_PROTOCOL, [
                'api_secret',
                'True',
                'False',
                '',
                'some_id',
                ''
            ])
        source = Source('orig1', SourceType.BIG_QUERY, [])
        execution = Execution(_account_config, source, destination)
        uploader.do_process(Batch(execution, [{
            'app_instance_id': '123',
            'name': 'event_name',
            'value': '42',
            'important_event': 'False'
        }]))

        assert m.call_count == 1
        assert m.last_request.json()['events'][0]['params']['value'] == '42'


def test_succesful_app_event_call_with_user_id(uploader):
    with requests_mock.Mocker() as m:
        m.post(requests_mock.ANY, status_code=204)
        destination = Destination(
            'dest1', DestinationType.GA_4_MEASUREMENT_PROTOCOL, [
                'api_secret',
                'True',
                'False',
                '',
                'some_id',
                ''
            ])
        source = Source('orig1', SourceType.BIG_QUERY, [])
        execution = Execution(_account_config, source, destination)
        uploader.do_process(Batch(execution, [{
            'app_instance_id': '123',
            'name': 'event_name',
            'value': '42',
            'user_id': 'Id42'
        }]))

        assert m.call_count == 1
        assert m.last_request.json()['user_id'] == 'Id42'


def test_succesful_web_user_property_call(uploader):
    with requests_mock.Mocker() as m:
        m.post(requests_mock.ANY, status_code=204)
        destination = Destination(
            'dest1', DestinationType.GA_4_MEASUREMENT_PROTOCOL, [
                'api_secret',
                'False',
                'True',
                '',
                '',
                'some_id'
            ])
        source = Source('orig1', SourceType.BIG_QUERY, [])
        execution = Execution(_account_config, source, destination)
        uploader.do_process(Batch(execution, [{
            'user_ltv': '42',
            'client_id': 'some_id'
        },
        {
            'user_will_churn': 'Maybe',
            'client_id': 'some_id'
        }
        ]))

        assert m.call_count == 2
        assert m.last_request.json(
        )['userProperties']['user_will_churn']['value'] == 'Maybe'

def test_succesful_web_user_property_call_with_user_id(uploader):
    with requests_mock.Mocker() as m:
        m.post(requests_mock.ANY, status_code=204)
        destination = Destination(
            'dest1', DestinationType.GA_4_MEASUREMENT_PROTOCOL, [
                'api_secret',
                'False',
                'True',
                '',
                '',
                'some_id'
            ])
        source = Source('orig1', SourceType.BIG_QUERY, [])
        execution = Execution(_account_config, source, destination)
        uploader.do_process(Batch(execution, [{
            'user_ltv': '42',
            'user_id': 'Id42',
            'client_id': 'someId'
        }
        ]))

        assert m.call_count == 1
        assert m.last_request.json(
        )['user_id'] == 'Id42'


def test_unsuccessful_api_call(uploader, error_notifier):
    with requests_mock.Mocker() as m:
        m.post(requests_mock.ANY, status_code=500)
        destination = Destination(
            'dest1', DestinationType.GA_4_MEASUREMENT_PROTOCOL, [
                'api_secret',
                'False',
                'True',
                '',
                '',
                'some_id'
            ])
        source = Source('orig1', SourceType.BIG_QUERY, ['',''])
        execution = Execution(_account_config, source, destination)
        uploader.do_process(Batch(execution, [{
            'user_ltv': '42',
            'user_id': 'Id42',
            'client_id': 'someId'
        }
        ]))


def test_unsuccessful_null_api_secret(uploader, error_notifier):
    with requests_mock.Mocker() as m:
        m.post(requests_mock.ANY, status_code=500)
        destination = Destination(
            'dest1', DestinationType.GA_4_MEASUREMENT_PROTOCOL, [
                '',
                'False',
                'True',
                '',
                '',
                'some_id'
            ])
        source = Source('orig1', SourceType.BIG_QUERY, ['',''])
        execution = Execution(_account_config, source, destination)
        with pytest.raises(ValueError, match='GA4 MP should be called with a non-null api_secret'):
            uploader.do_process(Batch(execution, [{
                'client_id': '123',
                'name': 'event_name',
                'value': '42',
                'important_event': 'False'
            }]))

def test_succesful_app_event_call_with_timestamp(uploader):
    with requests_mock.Mocker() as m:
        m.post(requests_mock.ANY, status_code=204)
        destination = Destination(
            'dest1', DestinationType.GA_4_MEASUREMENT_PROTOCOL, [
                'api_secret',
                'True',
                'False',
                '',
                'some_id',
                ''
            ])
        source = Source('orig1', SourceType.BIG_QUERY, [])
        execution = Execution(_account_config, source, destination)
        uploader.do_process(Batch(execution, [{
            'app_instance_id': '123',
            'name': 'event_name',
            'value': '42',
            'important_event': 'False',
            'timestamp_micros': 123123123
        }]))

        assert m.call_count == 1
        assert m.last_request.json()['timestamp_micros'] == 123123123

def test_succesful_filter_out_nulls(uploader):
    with requests_mock.Mocker() as m:
        m.post(requests_mock.ANY, status_code=204)
        destination = Destination(
            'dest1', DestinationType.GA_4_MEASUREMENT_PROTOCOL, [
                'api_secret',
                'True',
                'False',
                '',
                'some_id',
                ''
            ])
        source = Source('orig1', SourceType.BIG_QUERY, [])
        execution = Execution(_account_config, source, destination)
        uploader.do_process(Batch(execution, [{
            'app_instance_id': '123',
            'name': 'event_name',
            'empty_param': '',
            'important_event': 'False',
            'null_param': None
        }
        ]))

        assert m.call_count == 1
        assert 'empty_param' not in m.last_request.json().keys()
        assert 'null_param' not in m.last_request.json().keys()