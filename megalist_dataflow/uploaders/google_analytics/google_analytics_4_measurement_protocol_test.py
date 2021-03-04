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

from uploaders.google_analytics.google_analytics_4_measurement_protocol import GoogleAnalytics4MeasurementProtocolUploaderDoFn
from models.execution import Execution, SourceType, DestinationType, Source, AccountConfig, Destination, Batch

import requests
import requests_mock

from unittest import mock


_account_config = AccountConfig('account_id', False, 'ga_account_id', '', '')


@pytest.fixture
def uploader():
    return GoogleAnalytics4MeasurementProtocolUploaderDoFn()


def test_exception_event_and_user_property(uploader, caplog):
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
        source = Source('orig1', SourceType.BIG_QUERY, [])
        execution = Execution(_account_config, source, destination)
        with pytest.raises(ValueError, match="GA4 MP should be called either for sending events"):
            next(uploader.process(Batch(execution, [])))


def test_exception_no_event_nor_user_property(uploader, caplog):
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
        source = Source('orig1', SourceType.BIG_QUERY, [])
        execution = Execution(_account_config, source, destination)
        with pytest.raises(ValueError, match="GA4 MP should be called either for sending events"):
            next(uploader.process(Batch(execution, [])))


def test_exception_app_and_web(uploader, caplog):
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
        source = Source('orig1', SourceType.BIG_QUERY, [])
        execution = Execution(_account_config, source, destination)
        with pytest.raises(ValueError, match="GA4 MP should be called either with a firebase_app_id"):
            next(uploader.process(Batch(execution, [{
                'name': 'event_name',
            }])))


def test_exception_no_id(uploader, caplog):
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
        source = Source('orig1', SourceType.BIG_QUERY, [])
        execution = Execution(_account_config, source, destination)
        with pytest.raises(ValueError, match="GA4 MP should be called either with a firebase_app_id"):
            next(uploader.process(Batch(execution, [{
                'name': 'event_name',
                'value': '123'
            }])))


def test_succesful_app_event_call(uploader, caplog):
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
        next(uploader.process(Batch(execution, [{
            'firebase_app_id': '123',
            'name': 'event_name',
            'value': '42',
            'important_event': 'False'
        }])))

        assert m.call_count == 1
        assert m.last_request.json()["events"][0]["params"]["value"] == '42'


def test_succesful_web_user_property_call(uploader, caplog):
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
        next(uploader.process(Batch(execution, [{
            'user_ltv': '42'
        },
        {
            'user_will_churn': 'Maybe'
        }
        ])))

        assert m.call_count == 2
        assert m.last_request.json(
        )["userProperties"]["user_will_churn"]["value"] == 'Maybe'
