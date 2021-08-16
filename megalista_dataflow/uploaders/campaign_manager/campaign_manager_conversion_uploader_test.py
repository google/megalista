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
import math
import time
import logging

from apache_beam.options.value_provider import StaticValueProvider
from uploaders.campaign_manager.campaign_manager_conversion_uploader import CampaignManagerConversionUploaderDoFn
from models.execution import AccountConfig
from models.execution import Destination
from models.execution import DestinationType
from models.execution import Execution
from models.execution import Source
from models.execution import SourceType
from models.execution import Batch
from models.oauth_credentials import OAuthCredentials
import pytest

_account_config = AccountConfig(mcc=False,
                                campaign_manager_account_id='dcm_profile_id',
                                google_ads_account_id='',
                                google_analytics_account_id='',
                                app_id='')


@pytest.fixture
def uploader(mocker):
    credential_id = StaticValueProvider(str, 'id')
    secret = StaticValueProvider(str, 'secret')
    access = StaticValueProvider(str, 'access')
    refresh = StaticValueProvider(str, 'refresh')
    credentials = OAuthCredentials(credential_id, secret, access, refresh)

    return CampaignManagerConversionUploaderDoFn(credentials)


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
        'gclid': '123'
    }, {
        'gclid': '456'
    }]), current_time)

    expected_body = {
        'conversions': [{
            'gclid': '123',
            'floodlightActivityId': floodlight_activity_id,
            'floodlightConfigurationId': floodlight_configuration_id,
            'ordinal': math.floor(current_time * 10e5),
            'timestampMicros': math.floor(current_time * 10e5)
        }, {
            'gclid': '456',
            'floodlightActivityId': floodlight_activity_id,
            'floodlightConfigurationId': floodlight_configuration_id,
            'ordinal': math.floor(current_time * 10e5),
            'timestampMicros': math.floor(current_time * 10e5)
        }],
    }

    uploader._get_dcm_service().conversions().batchinsert.assert_any_call(
        profileId='dcm_profile_id', body=expected_body)


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
            'ordinal': math.floor(current_time * 10e5),
            'timestampMicros': math.floor(current_time * 10e5)
        }],
    }

    uploader._get_dcm_service().conversions().batchinsert.assert_any_call(
        profileId='dcm_profile_id', body=expected_body)


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
            'ordinal': math.floor(current_time * 10e5),
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
        profileId='dcm_profile_id', body=expected_body)


def test_error_on_api_call(mocker, uploader, caplog):
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

    source = Source('orig1', SourceType.BIG_QUERY, ('dt1', 'buyers'))
    destination = Destination(
        'dest1', DestinationType.CM_OFFLINE_CONVERSION, ['a', 'b'])
    execution = Execution(_account_config, source, destination)

    uploader._do_process(Batch(execution, [{'gclid': '123'}]), time.time())

    assert 'Error(s) inserting conversions:' in caplog.text
    assert '[123]: error_returned' in caplog.text
