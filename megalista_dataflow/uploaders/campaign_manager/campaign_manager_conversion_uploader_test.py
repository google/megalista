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
import decimal
import logging
import math
import time
from datetime import datetime

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
from uploaders.campaign_manager.campaign_manager_conversion_uploader import CampaignManagerConversionUploaderDoFn


_dcm_profile_id = '123456789'
_account_config = AccountConfig(mcc=False,
                                campaign_manager_profile_id=_dcm_profile_id,
                                google_ads_account_id='',
                                google_analytics_account_id='',
                                app_id='')


@pytest.fixture
def error_notifier():
    return MockErrorNotifier()

@pytest.fixture
def uploader(error_notifier):
    credential_id = StaticValueProvider(str, 'id')
    secret = StaticValueProvider(str, 'secret')
    access = StaticValueProvider(str, 'access')
    refresh = StaticValueProvider(str, 'refresh')
    credentials = OAuthCredentials(credential_id, secret, access, refresh)

    return CampaignManagerConversionUploaderDoFn(credentials,
                                                 ErrorHandler(DestinationType.CM_OFFLINE_CONVERSION, error_notifier))


def test_get_service(uploader):
    assert uploader._get_dcm_service() is not None


def test_conversion_upload(mocker, uploader):
    mocker.patch.object(uploader, '_get_dcm_service')

    floodlight_activity_id = 'floodlight_activity_id'
    floodlight_configuration_id = 'floodlight_configuration_id'

    source = Source('orig1', SourceType.BIG_QUERY, ('dt1', 'buyers'))
    destination = Destination(
        'dest1',
        DestinationType.CM_OFFLINE_CONVERSION,
        (floodlight_activity_id, floodlight_configuration_id))

    execution = Execution(_account_config, source, destination)

    current_time = time.time()

    uploader._do_process(Batch(execution, [{
        'gclid': '123',
        'timestamp': '2021-11-30T12:00:00.000'
    }, {
        'gclid': '456'
    }]), current_time)

    # convert 2021-11-30T12:00:00.000 to timestampMicros
    timestamp_micros = math.floor(datetime.strptime('2021-11-30T12:00:00.000', '%Y-%m-%dT%H:%M:%S.%f').timestamp() * 10e5)

    expected_body = {
        'conversions': [{
            'gclid': '123',
            'floodlightActivityId': floodlight_activity_id,
            'floodlightConfigurationId': floodlight_configuration_id,
            'ordinal': str(math.floor(current_time * 10e5)),
            'timestampMicros': timestamp_micros,
            'quantity': 1
        }, {
            'gclid': '456',
            'floodlightActivityId': floodlight_activity_id,
            'floodlightConfigurationId': floodlight_configuration_id,
            'ordinal': str(math.floor(current_time * 10e5)),
            'timestampMicros': math.floor(current_time * 10e5),
            'quantity': 1
        }],
    }

    uploader._get_dcm_service().conversions().batchinsert.assert_any_call(
        profileId=_dcm_profile_id, body=expected_body)
    
def test_conversion_upload_with_ordinal(mocker, uploader):
    mocker.patch.object(uploader, '_get_dcm_service')

    floodlight_activity_id = 'floodlight_activity_id'
    floodlight_configuration_id = 'floodlight_configuration_id'

    source = Source('orig1', SourceType.BIG_QUERY, ('dt1', 'buyers'))
    destination = Destination(
        'dest1',
        DestinationType.CM_OFFLINE_CONVERSION,
        (floodlight_activity_id, floodlight_configuration_id))

    execution = Execution(_account_config, source, destination)

    current_time = time.time()

    uploader._do_process(Batch(execution, [{
        'gclid': '123',
        'timestamp': '2021-11-30T12:00:00.000',
        'ordinal': '0927252-8'
    }, {
        'gclid': '456'
    }]), current_time)

    # convert 2021-11-30T12:00:00.000 to timestampMicros
    timestamp_micros = math.floor(datetime.strptime('2021-11-30T12:00:00.000', '%Y-%m-%dT%H:%M:%S.%f').timestamp() * 10e5)

    expected_body = {
        'conversions': [{
            'gclid': '123',
            'floodlightActivityId': floodlight_activity_id,
            'floodlightConfigurationId': floodlight_configuration_id,
            'ordinal': '0927252-8',
            'timestampMicros': timestamp_micros,
            'quantity': 1
        }, {
            'gclid': '456',
            'floodlightActivityId': floodlight_activity_id,
            'floodlightConfigurationId': floodlight_configuration_id,
            'ordinal': str(math.floor(current_time * 10e5)),
            'timestampMicros': math.floor(current_time * 10e5),
            'quantity': 1
        }],
    }

    uploader._get_dcm_service().conversions().batchinsert.assert_any_call(
        profileId=_dcm_profile_id, body=expected_body)

def test_conversion_upload_with_quantity(mocker, uploader):
    mocker.patch.object(uploader, '_get_dcm_service')
    floodlight_activity_id = 'floodlight_activity_id'
    floodlight_configuration_id = 'floodlight_configuration_id'
    source = Source('orig1', SourceType.BIG_QUERY, ('dt1', 'buyers'))
    destination = Destination(
        'dest1',
        DestinationType.CM_OFFLINE_CONVERSION,
        (floodlight_activity_id, floodlight_configuration_id))
    execution = Execution(_account_config, source, destination)
    current_time = time.time()
    uploader._do_process(Batch(execution, [{
        'gclid': '123',
        'timestamp': '2021-11-30T12:00:00.000',
        'quantity': 50
    }, {
        'gclid': '456',
        'quantity': 200
    }]), current_time)

    # convert 2021-11-30T12:00:00.000 to timestampMicros
    timestamp_micros = math.floor(datetime.strptime('2021-11-30T12:00:00.000', '%Y-%m-%dT%H:%M:%S.%f').timestamp() * 10e5)

    expected_body = {
        'conversions': [{
            'gclid': '123',
            'floodlightActivityId': floodlight_activity_id,
            'floodlightConfigurationId': floodlight_configuration_id,
            'ordinal': str(math.floor(current_time * 10e5)),
            'timestampMicros': timestamp_micros,
            'quantity': 50
        }, {
            'gclid': '456',
            'floodlightActivityId': floodlight_activity_id,
            'floodlightConfigurationId': floodlight_configuration_id,
            'ordinal': str(math.floor(current_time * 10e5)),
            'timestampMicros': math.floor(current_time * 10e5),
            'quantity': 200
        }],
    }

    uploader._get_dcm_service().conversions().batchinsert.assert_any_call(
        profileId=_dcm_profile_id, body=expected_body)

def test_conversion_upload_match_id(mocker, uploader):
    mocker.patch.object(uploader, '_get_dcm_service', autospec=True)

    floodlight_activity_id = 'floodlight_activity_id'
    floodlight_configuration_id = 'floodlight_configuration_id'

    source = Source('orig1', SourceType.BIG_QUERY, ('dt1', 'buyers'))
    destination = Destination(
        'dest1',
        DestinationType.CM_OFFLINE_CONVERSION,
        (floodlight_activity_id, floodlight_configuration_id))
    execution = Execution(_account_config, source, destination)
    current_time = time.time()

    mocker.patch.object(time, 'time')
    time.time.return_value = current_time

    uploader._do_process(Batch(execution, [{'matchId': 'abc'}]), current_time)

    expected_body = {
        'conversions': [{
            'matchId': 'abc',
            'floodlightActivityId': floodlight_activity_id,
            'floodlightConfigurationId': floodlight_configuration_id,
            'ordinal': str(math.floor(current_time * 10e5)),
            'timestampMicros': math.floor(current_time * 10e5),
            'quantity': 1
        }],
    }

    uploader._get_dcm_service().conversions().batchinsert.assert_any_call(
        profileId=_dcm_profile_id, body=expected_body)


def test_conversion_upload_match_id_additional_fields(mocker, uploader):
    mocker.patch.object(uploader, '_get_dcm_service', autospec=True)

    floodlight_activity_id = 'floodlight_activity_id'
    floodlight_configuration_id = 'floodlight_configuration_id'

    source = Source('orig1', SourceType.BIG_QUERY, ('dt1', 'buyers'))
    destination = Destination(
        'dest1',
        DestinationType.CM_OFFLINE_CONVERSION,
        (floodlight_activity_id, floodlight_configuration_id))
    execution = Execution(_account_config, source, destination)
    current_time = time.time()

    mocker.patch.object(time, 'time')
    time.time.return_value = current_time

    conversions_input = [{
        'matchId': 'abc',
        'value': 1,
        'quantity': 2,
        'customVariables': [{
            'type': 'U1',
            'value': "5.6",
        }, {
            'type': 'U2',
            'value': "abcd",
        }]
    }]

    expected_body = {
        'conversions': [{
            'matchId': 'abc',
            'floodlightActivityId': floodlight_activity_id,
            'floodlightConfigurationId': floodlight_configuration_id,
            'ordinal': str(math.floor(current_time * 10e5)),
            'timestampMicros': math.floor(current_time * 10e5),
            'value': 1,
            'quantity': 2,
            'customVariables': [{
                'type': 'U1',
                'value': "5.6",
                'kind': 'dfareporting#customFloodlightVariable',
            }, {
                'type': 'U2',
                'value': "abcd",
                'kind': 'dfareporting#customFloodlightVariable',
            }]
        }],
    }

    uploader._do_process(Batch(execution, conversions_input), current_time)

    uploader._get_dcm_service().conversions().batchinsert.assert_any_call(
        profileId=_dcm_profile_id, body=expected_body)


def test_conversion_upload_decimal_value(mocker, uploader):
    mocker.patch.object(uploader, '_get_dcm_service', autospec=True)

    floodlight_activity_id = 'floodlight_activity_id'
    floodlight_configuration_id = 'floodlight_configuration_id'

    source = Source('orig1', SourceType.BIG_QUERY, ('dt1', 'buyers'))
    destination = Destination(
        'dest1',
        DestinationType.CM_OFFLINE_CONVERSION,
        (floodlight_activity_id, floodlight_configuration_id))
    execution = Execution(_account_config, source, destination)
    current_time = time.time()

    mocker.patch.object(time, 'time')
    time.time.return_value = current_time

    conversions_input = [{
        'gclid': 'abc',
        'value': decimal.Decimal('540.12'),
    }]

    expected_body = {
        'conversions': [{
            'gclid': 'abc',
            'floodlightActivityId': floodlight_activity_id,
            'floodlightConfigurationId': floodlight_configuration_id,
            'ordinal': str(math.floor(current_time * 10e5)),
            'timestampMicros': math.floor(current_time * 10e5),
            'value': 540.12,
            'quantity': 1
        }],
    }

    uploader._do_process(Batch(execution, conversions_input), current_time)

    uploader._get_dcm_service().conversions().batchinsert.assert_any_call(
        profileId=_dcm_profile_id, body=expected_body)


def test_error_on_api_call(mocker, uploader, caplog, error_notifier):
    caplog.set_level(logging.INFO, 'megalista.CampaignManagerConversionsUploader')
    mocker.patch.object(uploader, '_get_dcm_service')
    service = mocker.MagicMock()
    uploader._get_dcm_service.return_value = service

    service.conversions().batchinsert().execute.return_value = {
        'hasFailures': True,
        'status': [{
            'errors': [{
                'code': '123',
                'message': 'error_returned'
            }]
        }]
    }

    source = Source('orig1', SourceType.BIG_QUERY, ['dt1', 'buyers'])
    destination = Destination(
        'dest1', DestinationType.CM_OFFLINE_CONVERSION, ['a', 'b'])
    execution = Execution(_account_config, source, destination)

    uploader._do_process(Batch(execution, [{'gclid': '123'}]), time.time())
    uploader.finish_bundle()

    assert 'Error(s) inserting conversions:' in caplog.text
    assert '[123]: error_returned' in caplog.text

    assert error_notifier.were_errors_sent
