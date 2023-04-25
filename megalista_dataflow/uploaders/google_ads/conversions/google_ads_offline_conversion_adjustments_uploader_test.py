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
from unittest.mock import MagicMock

import pytest
from apache_beam.options.value_provider import StaticValueProvider

from error.error_handling import ErrorHandler
from error.error_handling_test import MockErrorNotifier
from models.execution import AccountConfig
from models.execution import Batch
from models.execution import Destination
from models.execution import DestinationType
from models.execution import Execution
from models.execution import Source
from models.execution import SourceType
from models.oauth_credentials import OAuthCredentials
from uploaders.google_ads.conversions.google_ads_offline_conversion_adjustments_uploader import (
    GoogleAdsOfflineAdjustmentUploaderDoFn,
)

_account_config = AccountConfig('123-45567-890', False, 'ga_account_id', '', '')


@pytest.fixture()
def error_notifier():
    return MockErrorNotifier()


@pytest.fixture
def uploader(mocker, error_notifier):
    mocker.patch('google.ads.googleads.client.GoogleAdsClient')
    mocker.patch('google.ads.googleads.oauth2')
    credential_id = StaticValueProvider(str, 'id')
    secret = StaticValueProvider(str, 'secret')
    access = StaticValueProvider(str, 'access')
    refresh = StaticValueProvider(str, 'refresh')
    credentials = OAuthCredentials(credential_id, secret, access, refresh)
    return GoogleAdsOfflineAdjustmentUploaderDoFn(
        credentials,
        StaticValueProvider(str, 'devtoken'),
        ErrorHandler(DestinationType.ADS_OFFLINE_CONVERSION_ADJUSTMENT, error_notifier),
    )


def test_get_service(mocker, uploader):
    assert uploader._get_oca_service(mocker.ANY) is not None


def arrange_conversion_resource_name_api_call(
    mocker, uploader, conversion_resource_name
):
    mocker.patch.object(uploader, '_get_ads_service')

    resource_name_result = MagicMock()
    resource_name_result.conversion_action.resource_name = conversion_resource_name

    resource_name_batch_response = MagicMock()
    resource_name_batch_response.results = [resource_name_result]

    uploader._get_ads_service.return_value.search_stream.return_value = [
        resource_name_batch_response
    ]


def test_conversion_upload_with_gclids(mocker, uploader):
    # arrange
    conversion_resource_name = 'user_list_resouce'
    arrange_conversion_resource_name_api_call(
        mocker, uploader, conversion_resource_name
    )

    mocker.patch.object(uploader, '_get_oca_service')
    conversion_name = 'user_list'
    destination = Destination(
        'dest1', DestinationType.ADS_OFFLINE_CONVERSION_ADJUSTMENT, ['user_list']
    )
    source = Source('orig1', SourceType.BIG_QUERY, ['dt1', 'buyers'])
    execution = Execution(_account_config, source, destination)

    time1 = '2023-04-01T14:13:55.0005'
    time1_result = '2023-04-01 14:13:55-03:00'

    time2 = '2023-04-02T14:13:55.0005'
    time2_result = '2023-04-02 14:13:55-03:00'

    time3 = '2023-04-03T13:13:55.0005'
    time3_result = '2023-04-03 13:13:55-03:00'

    time4 = '2023-04-04T13:13:55.0005'
    time4_result = '2023-04-04 13:13:55-03:00'

    element1 = {
        'order_id': '',
        'gclid': '123',
        'time': time1,
        'adjustment_time': time2,
        'adjustment_type': 'RESTATEMENT',
        'amount': '456',
    }
    element2 = {
        'order_id': '',
        'gclid': '789',
        'time': time3,
        'adjustment_time': time4,
        'adjustment_type': 'RESTATEMENT',
        'amount': '101',
    }

    batch = Batch(execution, [element1, element2])

    gclid_result_mock1 = MagicMock()
    gclid_result_mock1.gclid_date_time_pair.gclid = None

    gclid_result_mock2 = MagicMock()
    gclid_result_mock2.gclid_date_time_pair.gclid = '789'

    upload_return_mock = MagicMock()
    upload_return_mock.results = [gclid_result_mock1, gclid_result_mock2]
    uploader._get_oca_service.return_value.upload_conversion_adjustments.return_value = (
        upload_return_mock
    )

    # act
    successful_uploaded_gclids_batch = uploader.process(batch)[0]
    uploader.finish_bundle()

    # assert
    assert len(successful_uploaded_gclids_batch.elements) == 1
    assert successful_uploaded_gclids_batch.elements[0] == element2

    uploader._get_ads_service.return_value.search_stream.assert_called_once_with(
        customer_id='12345567890',
        query=f"SELECT conversion_action.resource_name FROM conversion_action WHERE conversion_action.name = '{conversion_name}'",
    )

    uploader._get_oca_service.return_value.upload_conversion_adjustments.assert_called_once_with(
        request={
            'customer_id': '12345567890',
            'partial_failure': True,
            'validate_only': False,
            'conversion_adjustments': [
                {
                    'adjustment_type': 'RESTATEMENT',
                    'restatement_value': {
                        'adjusted_value': 456.0,
                        'currency_code': None,
                    },
                    'conversion_action': conversion_resource_name,
                    'adjustment_date_time': time2_result,
                    'gclid_date_time_pair': {
                        'gclid': '123',
                        'conversion_date_time': time1_result,
                    },
                },
                {
                    'adjustment_type': 'RESTATEMENT',
                    'restatement_value': {
                        'adjusted_value': 101.0,
                        'currency_code': None,
                    },
                    'conversion_action': conversion_resource_name,
                    'adjustment_date_time': time4_result,
                    'gclid_date_time_pair': {
                        'gclid': '789',
                        'conversion_date_time': time3_result,
                    },
                },
            ],
        }
    )


def test_conversion_upload_with_order_ids(mocker, uploader):
    # arrange
    conversion_resource_name = 'user_list_resouce'
    arrange_conversion_resource_name_api_call(
        mocker, uploader, conversion_resource_name
    )

    mocker.patch.object(uploader, '_get_oca_service')
    conversion_name = 'user_list'
    destination = Destination(
        'dest1', DestinationType.ADS_OFFLINE_CONVERSION_ADJUSTMENT, ['user_list']
    )
    source = Source('orig1', SourceType.BIG_QUERY, ['dt1', 'buyers'])
    execution = Execution(_account_config, source, destination)

    time2 = '2023-04-02T14:13:55.0005'
    time2_result = '2023-04-02 14:13:55-03:00'

    time4 = '2023-04-04T13:13:55.0005'
    time4_result = '2023-04-04 13:13:55-03:00'

    element1 = {
        'order_id': 'AAA123',
        'gclid': '',
        'time': '',
        'adjustment_time': time2,
        'adjustment_type': 'RESTATEMENT',
        'amount': '456',
    }
    element2 = {
        'order_id': 'AAA456',
        'gclid': '',
        'time': '',
        'adjustment_time': time4,
        'adjustment_type': 'RESTATEMENT',
        'amount': '101',
    }

    batch = Batch(execution, [element1, element2])

    order_id_result_mock1 = MagicMock()
    order_id_result_mock1.order_id = None

    order_id_result_mock2 = MagicMock()
    order_id_result_mock2.order_id = 'AAA456'

    upload_return_mock = MagicMock()
    upload_return_mock.results = [order_id_result_mock1, order_id_result_mock2]
    uploader._get_oca_service.return_value.upload_conversion_adjustments.return_value = (
        upload_return_mock
    )

    # act
    successful_uploaded_gclids_batch = uploader.process(batch)[0]
    uploader.finish_bundle()

    # assert
    assert len(successful_uploaded_gclids_batch.elements) == 1
    assert successful_uploaded_gclids_batch.elements[0] == element2

    uploader._get_ads_service.return_value.search_stream.assert_called_once_with(
        customer_id='12345567890',
        query=f"SELECT conversion_action.resource_name FROM conversion_action WHERE conversion_action.name = '{conversion_name}'",
    )

    uploader._get_oca_service.return_value.upload_conversion_adjustments.assert_called_once_with(
        request={
            'customer_id': '12345567890',
            'partial_failure': True,
            'validate_only': False,
            'conversion_adjustments': [
                {
                    'adjustment_type': 'RESTATEMENT',
                    'restatement_value': {
                        'adjusted_value': 456.0,
                        'currency_code': None,
                    },
                    'conversion_action': conversion_resource_name,
                    'adjustment_date_time': time2_result,
                    'order_id': 'AAA123',
                },
                {
                    'adjustment_type': 'RESTATEMENT',
                    'restatement_value': {
                        'adjusted_value': 101.0,
                        'currency_code': None,
                    },
                    'conversion_action': conversion_resource_name,
                    'adjustment_date_time': time4_result,
                    'order_id': 'AAA456',
                },
            ],
        }
    )


def test_conversion_upload_with_mixed_ids(mocker, uploader):
    # arrange
    conversion_resource_name = 'user_list_resouce'
    arrange_conversion_resource_name_api_call(
        mocker, uploader, conversion_resource_name
    )

    mocker.patch.object(uploader, '_get_oca_service')
    conversion_name = 'user_list'
    destination = Destination(
        'dest1', DestinationType.ADS_OFFLINE_CONVERSION_ADJUSTMENT, ['user_list']
    )
    source = Source('orig1', SourceType.BIG_QUERY, ['dt1', 'buyers'])
    execution = Execution(_account_config, source, destination)

    time1 = '2023-04-01T14:13:55.0005'
    time1_result = '2023-04-01 14:13:55-03:00'

    time2 = '2023-04-02T14:13:55.0005'
    time2_result = '2023-04-02 14:13:55-03:00'

    time4 = '2023-04-04T13:13:55.0005'
    time4_result = '2023-04-04 13:13:55-03:00'

    element1 = {
        'order_id': 'AAA123',
        'gclid': '',
        'time': '',
        'adjustment_time': time2,
        'adjustment_type': 'RESTATEMENT',
        'amount': '456',
    }
    element2 = {
        'order_id': '',
        'gclid': '123',
        'time': time1,
        'adjustment_time': time4,
        'adjustment_type': 'RESTATEMENT',
        'amount': '101',
    }

    batch = Batch(execution, [element1, element2])

    order_id_result_mock1 = MagicMock()
    order_id_result_mock1.order_id = 'AAA123'

    order_id_result_mock2 = MagicMock()
    order_id_result_mock2.gclid_date_time_pair.gclid = '123'

    upload_return_mock = MagicMock()
    upload_return_mock.results = [order_id_result_mock1, order_id_result_mock2]
    uploader._get_oca_service.return_value.upload_conversion_adjustments.return_value = (
        upload_return_mock
    )

    # act
    successful_uploaded_gclids_batch = uploader.process(batch)[0]
    uploader.finish_bundle()

    # assert
    assert len(successful_uploaded_gclids_batch.elements) == 2
    assert successful_uploaded_gclids_batch.elements[0] == element2
    assert successful_uploaded_gclids_batch.elements[1] == element1

    uploader._get_ads_service.return_value.search_stream.assert_called_once_with(
        customer_id='12345567890',
        query=f"SELECT conversion_action.resource_name FROM conversion_action WHERE conversion_action.name = '{conversion_name}'",
    )

    uploader._get_oca_service.return_value.upload_conversion_adjustments.assert_called_once_with(
        request={
            'customer_id': '12345567890',
            'partial_failure': True,
            'validate_only': False,
            'conversion_adjustments': [
                {
                    'adjustment_type': 'RESTATEMENT',
                    'restatement_value': {
                        'adjusted_value': 456.0,
                        'currency_code': None,
                    },
                    'conversion_action': conversion_resource_name,
                    'adjustment_date_time': time2_result,
                    'order_id': 'AAA123',
                },
                {
                    'adjustment_type': 'RESTATEMENT',
                    'restatement_value': {
                        'adjusted_value': 101.0,
                        'currency_code': None,
                    },
                    'conversion_action': conversion_resource_name,
                    'adjustment_date_time': time4_result,
                    'gclid_date_time_pair': {
                        'gclid': '123',
                        'conversion_date_time': time1_result,
                    },
                },
            ],
        }
    )


def test_upload_with_ads_account_override(mocker, uploader):
    # arrange
    conversion_resource_name = 'user_list_resouce'
    arrange_conversion_resource_name_api_call(
        mocker, uploader, conversion_resource_name
    )

    mocker.patch.object(uploader, '_get_oca_service')
    conversion_name = 'user_list'
    destination = Destination(
        'dest1',
        DestinationType.ADS_OFFLINE_CONVERSION_ADJUSTMENT,
        ['user_list', '987-7654-123'],
    )
    source = Source('orig1', SourceType.BIG_QUERY, ['dt1', 'buyers'])
    execution = Execution(_account_config, source, destination)

    time1 = '2023-04-01T14:13:55.0005'
    time1_result = '2023-04-01 14:13:55-03:00'

    time2 = '2023-04-02T14:13:55.0005'
    time2_result = '2023-04-02 14:13:55-03:00'

    time3 = '2023-04-03T13:13:55.0005'
    time3_result = '2023-04-03 13:13:55-03:00'

    time4 = '2023-04-04T13:13:55.0005'
    time4_result = '2023-04-04 13:13:55-03:00'

    element1 = {
        'order_id': '',
        'gclid': '123',
        'time': time1,
        'adjustment_time': time2,
        'adjustment_type': 'RESTATEMENT',
        'amount': '456',
    }
    element2 = {
        'order_id': '',
        'gclid': '789',
        'time': time3,
        'adjustment_time': time4,
        'adjustment_type': 'RESTATEMENT',
        'amount': '101',
    }

    batch = Batch(execution, [element1, element2])

    gclid_result_mock1 = MagicMock()
    gclid_result_mock1.gclid_date_time_pair.gclid = None

    gclid_result_mock2 = MagicMock()
    gclid_result_mock2.gclid_date_time_pair.gclid = '789'

    upload_return_mock = MagicMock()
    upload_return_mock.results = [gclid_result_mock1, gclid_result_mock2]
    uploader._get_oca_service.return_value.upload_conversion_adjustments.return_value = (
        upload_return_mock
    )

    # act
    successful_uploaded_gclids_batch = uploader.process(batch)[0]
    uploader.finish_bundle()

    # assert
    assert len(successful_uploaded_gclids_batch.elements) == 1
    assert successful_uploaded_gclids_batch.elements[0] == element2

    uploader._get_ads_service.return_value.search_stream.assert_called_once_with(
        customer_id='9877654123',
        query=f"SELECT conversion_action.resource_name FROM conversion_action WHERE conversion_action.name = '{conversion_name}'",
    )

    uploader._get_oca_service.return_value.upload_conversion_adjustments.assert_called_once_with(
        request={
            'customer_id': '9877654123',
            'partial_failure': True,
            'validate_only': False,
            'conversion_adjustments': [
                {
                    'adjustment_type': 'RESTATEMENT',
                    'restatement_value': {
                        'adjusted_value': 456.0,
                        'currency_code': None,
                    },
                    'conversion_action': conversion_resource_name,
                    'adjustment_date_time': time2_result,
                    'gclid_date_time_pair': {
                        'gclid': '123',
                        'conversion_date_time': time1_result,
                    },
                },
                {
                    'adjustment_type': 'RESTATEMENT',
                    'restatement_value': {
                        'adjusted_value': 101.0,
                        'currency_code': None,
                    },
                    'conversion_action': conversion_resource_name,
                    'adjustment_date_time': time4_result,
                    'gclid_date_time_pair': {
                        'gclid': '789',
                        'conversion_date_time': time3_result,
                    },
                },
            ],
        }
    )


def test_should_not_notify_errors_when_api_call_is_successful(
    mocker, uploader, error_notifier
):
    # arrange
    conversion_resource_name = 'user_list_resouce'
    arrange_conversion_resource_name_api_call(
        mocker, uploader, conversion_resource_name
    )

    mocker.patch.object(uploader, '_get_oca_service')
    conversion_name = 'user_list'
    destination = Destination(
        'dest1', DestinationType.ADS_OFFLINE_CONVERSION_ADJUSTMENT, ['user_list']
    )
    source = Source('orig1', SourceType.BIG_QUERY, ['dt1', 'buyers'])
    execution = Execution(_account_config, source, destination)

    time1 = '2023-04-01T14:13:55.0005'

    element1 = {
        'order_id': '',
        'gclid': '123',
        'time': time1,
        'adjustment_time': time1,
        'adjustment_type': 'RESTATEMENT',
        'amount': '456',
    }

    batch = Batch(execution, [element1])

    gclid_result_mock1 = MagicMock()
    gclid_result_mock1.gclid = '123'

    upload_return_mock = MagicMock()
    upload_return_mock.results = [gclid_result_mock1]
    upload_return_mock.partial_failure_error = None
    uploader._get_oca_service.return_value.upload_conversion_adjustments.return_value = (
        upload_return_mock
    )

    # act
    uploader.process(batch)
    uploader.finish_bundle()

    uploader._get_ads_service.return_value.search_stream.assert_called_once_with(
        customer_id='12345567890',
        query=f"SELECT conversion_action.resource_name FROM conversion_action WHERE conversion_action.name = '{conversion_name}'",
    )

    assert error_notifier.were_errors_sent is False


def test_error_notification(mocker, uploader, error_notifier):
    # arrange
    conversion_resource_name = 'user_list_resouce'
    arrange_conversion_resource_name_api_call(
        mocker, uploader, conversion_resource_name
    )

    mocker.patch.object(uploader, '_get_oca_service')
    destination = Destination(
        'dest1', DestinationType.ADS_OFFLINE_CONVERSION_ADJUSTMENT, ['user_list']
    )
    source = Source('orig1', SourceType.BIG_QUERY, ['dt1', 'buyers'])
    execution = Execution(_account_config, source, destination)

    time1 = '2023-04-01T14:13:55.0005'
    element1 = {
        'order_id': '',
        'gclid': '123',
        'time': time1,
        'adjustment_time': time1,
        'adjustment_type': 'RESTATEMENT',
        'amount': '456',
    }
    batch = Batch(execution, [element1])

    error_message = 'Offline Conversion uploading failures'
    upload_return_mock = MagicMock()
    upload_return_mock.partial_failure_error.message = error_message
    uploader._get_oca_service.return_value.upload_conversion_adjustments.return_value = (
        upload_return_mock
    )

    # act
    uploader.process(batch)
    uploader.finish_bundle()

    # assert
    assert error_notifier.were_errors_sent is True
    assert (
        error_notifier.destination_type
        is DestinationType.ADS_OFFLINE_CONVERSION_ADJUSTMENT
    )
    assert error_notifier.errors_sent == {
        execution: f'Error on uploading offline conversion adjustments: {error_message}.'
    }


def test_conversion_upload_and_error_notification(mocker, uploader, error_notifier):
    """
    Scenario where some gclids are uploaded but some give errors
    """

    # arrange
    conversion_resource_name = 'user_list_resouce'
    arrange_conversion_resource_name_api_call(
        mocker, uploader, conversion_resource_name
    )

    mocker.patch.object(uploader, '_get_oca_service')
    conversion_name = 'user_list'
    destination = Destination(
        'dest1', DestinationType.ADS_OFFLINE_CONVERSION_ADJUSTMENT, ['user_list']
    )
    source = Source('orig1', SourceType.BIG_QUERY, ['dt1', 'buyers'])
    execution = Execution(_account_config, source, destination)

    time1 = '2023-04-01T14:13:55.0005'
    time1_result = '2023-04-01 14:13:55-03:00'

    time2 = '2023-04-02T14:13:55.0005'
    time2_result = '2023-04-02 14:13:55-03:00'

    time3 = '2023-04-03T13:13:55.0005'
    time3_result = '2023-04-03 13:13:55-03:00'

    time4 = '2023-04-04T13:13:55.0005'
    time4_result = '2023-04-04 13:13:55-03:00'

    element1 = {
        'order_id': '',
        'gclid': '123',
        'time': time1,
        'adjustment_time': time2,
        'adjustment_type': 'RESTATEMENT',
        'amount': '456',
    }
    element2 = {
        'order_id': '',
        'gclid': '789',
        'time': time3,
        'adjustment_time': time4,
        'adjustment_type': 'RESTATEMENT',
        'amount': '101',
    }
    batch = Batch(execution, [element1, element2])

    # gclid '123' returns as successful by the API, while gclid '789' does not.
    # in this scenario, it's expected that both are present in the API call,
    # but since gclid '789' is not returned as successful by the API, an error is sent through error_notifier

    error_message = 'Offline Conversion uploading failures'

    gclid_result_mock1 = MagicMock()
    gclid_result_mock1.gclid_date_time_pair.gclid = '123'

    upload_return_mock = MagicMock()
    upload_return_mock.results = [gclid_result_mock1]
    upload_return_mock.partial_failure_error.message = error_message
    uploader._get_oca_service.return_value.upload_conversion_adjustments.return_value = (
        upload_return_mock
    )

    # act
    successful_uploaded_gclids_batch = uploader.process(batch)[0]
    uploader.finish_bundle()

    # assert
    assert len(successful_uploaded_gclids_batch.elements) == 1
    assert successful_uploaded_gclids_batch.elements[0] == element1

    uploader._get_ads_service.return_value.search_stream.assert_called_once_with(
        customer_id='12345567890',
        query=f"SELECT conversion_action.resource_name FROM conversion_action WHERE conversion_action.name = '{conversion_name}'",
    )

    uploader._get_oca_service.return_value.upload_conversion_adjustments.assert_called_once_with(
        request={
            'customer_id': '12345567890',
            'partial_failure': True,
            'validate_only': False,
            'conversion_adjustments': [
                {
                    'adjustment_type': 'RESTATEMENT',
                    'restatement_value': {
                        'adjusted_value': 456.0,
                        'currency_code': None,
                    },
                    'conversion_action': conversion_resource_name,
                    'adjustment_date_time': time2_result,
                    'gclid_date_time_pair': {
                        'gclid': '123',
                        'conversion_date_time': time1_result,
                    },
                },
                {
                    'adjustment_type': 'RESTATEMENT',
                    'restatement_value': {
                        'adjusted_value': 101.0,
                        'currency_code': None,
                    },
                    'conversion_action': conversion_resource_name,
                    'adjustment_date_time': time4_result,
                    'gclid_date_time_pair': {
                        'gclid': '789',
                        'conversion_date_time': time3_result,
                    },
                },
            ],
        }
    )

    assert error_notifier.were_errors_sent is True
    assert (
        error_notifier.destination_type
        is DestinationType.ADS_OFFLINE_CONVERSION_ADJUSTMENT
    )
    assert error_notifier.errors_sent == {
        execution: f'Error on uploading offline conversion adjustments: {error_message}.'
    }
