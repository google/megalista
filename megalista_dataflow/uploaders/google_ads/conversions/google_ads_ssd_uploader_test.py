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
from models.execution import AccountConfig
from models.execution import Batch
from models.execution import Destination
from models.execution import DestinationType
from models.execution import Execution
from models.execution import Source
from models.execution import SourceType
from models.oauth_credentials import OAuthCredentials
from uploaders.google_ads.conversions.google_ads_ssd_uploader import GoogleAdsSSDUploaderDoFn

_account_config = AccountConfig('account_id', False, 'ga_account_id', '', '')
_mcc_account_config = AccountConfig('mcc_account_id', True, 'ga_account_id', '', '')

_conversion_name = 'ssd_conversion'
_external_upload_id = '123' #TODO(caiotomazelli): Remove, not being used
_should_hash = True

_time1 = '2020-04-09T14:13:55.0005'
_time1_result = '2020-04-09 14:13:55-03:00'

_time2 = '2020-04-09T13:13:55.0005'
_time2_result = '2020-04-09 13:13:55-03:00'

EMAIL_1 = 'a@a.com'
EMAIL_2 = 'b@b.com'
CURRENCY_BRL = 'BRL'
AMOUNT_1 = '123'
AMOUNT_2 = '234'

@pytest.fixture
def uploader(mocker):
    mocker.patch('google.ads.googleads.client.GoogleAdsClient')
    mocker.patch('google.ads.googleads.oauth2')
    _id = StaticValueProvider(str, 'id')
    secret = StaticValueProvider(str, 'secret')
    access = StaticValueProvider(str, 'access')
    refresh = StaticValueProvider(str, 'refresh')
    credentials = OAuthCredentials(_id, secret, access, refresh)
    return GoogleAdsSSDUploaderDoFn(credentials,
                                    StaticValueProvider(str, 'devtoken'),
                                    ErrorHandler(DestinationType.ADS_SSD_UPLOAD, MockErrorNotifier()))

@pytest.fixture
def ssd_batch():
    return create_batch(_account_config, '', '')

@pytest.fixture
def ssd_batch_with_account_and_currency_override():
    return create_batch(_account_config, 'currency_override', 'account_override')

@pytest.fixture
def ssd_batch_with_mcc_account_override():
    return create_batch(_mcc_account_config, '', '')

def create_batch(account_config, currency_override, account_override):
    source = Source('orig1', SourceType.BIG_QUERY, ('dt1', 'buyers'))
    destination = Destination('dest1', DestinationType.ADS_SSD_UPLOAD,
                              [_conversion_name, _external_upload_id, _should_hash, currency_override, account_override])
    execution = Execution(account_config, source, destination)

    return Batch(execution, [{
        'hashed_email': EMAIL_1,
        'time': _time1,
        'amount': AMOUNT_1
    }, {
        'hashed_email': EMAIL_2,
        'time': _time2,
        'amount': AMOUNT_2
    }])

def test_get_service(mocker, uploader):
    assert uploader._get_offline_user_data_job_service(mocker.ANY) is not None

def test_fail_missing_destination_metadata(uploader, mocker):
    mocker.patch.object(uploader, '_get_offline_user_data_job_service')
    source = Source('orig1', SourceType.BIG_QUERY, ('dt1', 'buyers'))
    destination = Destination('dest1', DestinationType.ADS_SSD_UPLOAD, ['1', '2'])
    execution = Execution(_account_config, source, destination)
    batch = Batch(execution, [])
    uploader.process(batch)
    uploader._get_offline_user_data_job_service.assert_not_called()

def test_conversion_upload(mocker, uploader, ssd_batch):
    mocker.patch.object(uploader, '_get_offline_user_data_job_service')
    mocker.patch.object(uploader, '_get_resource_name')

    resource_name = uploader._get_offline_user_data_job_service.return_value.create_offline_user_data_job.return_value.resource_name
    conversion_name_resource_name = uploader._get_resource_name.return_value

    uploader.process(ssd_batch)

    data_insertion_payload = {
        'resource_name': resource_name,
        'enable_partial_failure': False,
        'operations': [{
            'create': {
                'user_identifiers': [{
                    'hashed_email': EMAIL_1
                }],
                'transaction_attribute': {
                    'conversion_action': conversion_name_resource_name,
                    'currency_code': CURRENCY_BRL,
                    'transaction_amount_micros': AMOUNT_1,
                    'transaction_date_time': _time1_result
                }
            }
        }, {
            'create': {
                'user_identifiers': [{
                    'hashed_email': EMAIL_2
                }],
                'transaction_attribute': {
                    'conversion_action': conversion_name_resource_name,
                    'currency_code': CURRENCY_BRL,
                    'transaction_amount_micros': AMOUNT_2,
                    'transaction_date_time': _time2_result
                }
            }
        }]
    }

    uploader._get_offline_user_data_job_service.assert_called_with('account_id')
    uploader._get_offline_user_data_job_service.return_value.add_offline_user_data_job_operations.assert_any_call(request = data_insertion_payload)

def test_conversion_upload_account_and_currency_override(mocker, uploader, ssd_batch_with_account_and_currency_override):
    mocker.patch.object(uploader, '_get_offline_user_data_job_service')
    mocker.patch.object(uploader, '_get_resource_name')

    resource_name = uploader._get_offline_user_data_job_service.return_value.create_offline_user_data_job.return_value.resource_name
    conversion_name_resource_name = uploader._get_resource_name.return_value

    uploader.process(ssd_batch_with_account_and_currency_override)

    data_insertion_payload = {
        'resource_name': resource_name,
        'enable_partial_failure': False,
        'operations': [{
            'create': {
                'user_identifiers': [{
                    'hashed_email': EMAIL_1
                }],
                'transaction_attribute': {
                    'conversion_action': conversion_name_resource_name,
                    'currency_code': 'currency_override',
                    'transaction_amount_micros': AMOUNT_1,
                    'transaction_date_time': _time1_result
                }
            }
        }, {
            'create': {
                'user_identifiers': [{
                    'hashed_email': EMAIL_2
                }],
                'transaction_attribute': {
                    'conversion_action': conversion_name_resource_name,
                    'currency_code': 'currency_override',
                    'transaction_amount_micros': AMOUNT_2,
                    'transaction_date_time': _time2_result
                }
            }
        }]
    }

    uploader._get_offline_user_data_job_service.assert_called_with('account_override')
    uploader._get_offline_user_data_job_service.return_value.add_offline_user_data_job_operations.assert_any_call(request = data_insertion_payload)

def test_conversion_mcc_account_override(mocker, uploader, ssd_batch_with_mcc_account_override):
    mocker.patch.object(uploader, '_get_offline_user_data_job_service')
    mocker.patch.object(uploader, '_get_resource_name')

    resource_name = uploader._get_offline_user_data_job_service.return_value.create_offline_user_data_job.return_value.resource_name
    conversion_name_resource_name = uploader._get_resource_name.return_value

    uploader.process(ssd_batch_with_mcc_account_override)

    data_insertion_payload = {
        'resource_name': resource_name,
        'enable_partial_failure': False,
        'operations': [{
            'create': {
                'user_identifiers': [{
                    'hashed_email': EMAIL_1
                }],
                'transaction_attribute': {
                    'conversion_action': conversion_name_resource_name,
                    'currency_code': CURRENCY_BRL,
                    'transaction_amount_micros': AMOUNT_1,
                    'transaction_date_time': _time1_result
                }
            }
        }, {
            'create': {
                'user_identifiers': [{
                    'hashed_email': EMAIL_2
                }],
                'transaction_attribute': {
                    'conversion_action': conversion_name_resource_name,
                    'currency_code': CURRENCY_BRL,
                    'transaction_amount_micros': AMOUNT_2,
                    'transaction_date_time': _time2_result
                }
            }
        }]
    }

    uploader._get_offline_user_data_job_service.assert_called_with('mcc_account_id')
    uploader._get_offline_user_data_job_service.return_value.add_offline_user_data_job_operations.assert_any_call(request = data_insertion_payload)