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
from re import A
from unittest.mock import ANY, MagicMock
from urllib import response

import pytest
from apache_beam.options.value_provider import StaticValueProvider

from error.error_handling import ErrorHandler
from error.error_handling_test import MockErrorNotifier
from models.execution import AccountConfig, Destination, DestinationType, Source, SourceType, Execution, Batch
from models.oauth_credentials import OAuthCredentials
from uploaders.display_video.customer_match.mobile_uploader import DisplayVideoCustomerMatchMobileUploaderDoFn
from uploaders.utils import MagicMockDict

_account_config = AccountConfig('account_id', False, 'ga_account_id', '', '')


@pytest.fixture
def error_notifier(mocker):
    return MockErrorNotifier()


@pytest.fixture
def uploader(mocker, error_notifier):
    mocker.patch('googleapiclient.discovery.build')
    mocker.patch('google.ads.googleads.oauth2')
    _id = StaticValueProvider(str, 'id')
    secret = StaticValueProvider(str, 'secret')
    access = StaticValueProvider(str, 'access')
    refresh = StaticValueProvider(str, 'refresh')
    credentials = OAuthCredentials(_id, secret, access, refresh)
    return DisplayVideoCustomerMatchMobileUploaderDoFn(credentials,
                                                            StaticValueProvider(
                                                                str, 'devtoken'),
                                                            ErrorHandler(
                                                                DestinationType.DV_CUSTOMER_MATCH_DEVICE_ID_UPLOAD, error_notifier))


def test_upload_add_users(mocker, uploader, error_notifier):

    mocker.patch.object(uploader, '_get_dv_audience_service')
    
    audience = MagicMock() 
    audience.firstAndThirdPartyAudienceId = 12345
    audience.displayName = 'list_name'

    uploader._get_dv_audience_service.return_value.list.return_value.execute.return_value = None
    uploader._get_dv_audience_service.return_value.create.return_value = audience
    
    uploader._get_dv_audience_service.return_value.editCustomerMatchMembers.return_value = MagicMock()

    destination = Destination(
        'dest1',
        DestinationType.DV_CUSTOMER_MATCH_DEVICE_ID_UPLOAD,
        ['advertiser_id', 'list_name', True, 1234]
    )

    source = Source('orig1', SourceType.BIG_QUERY, ['dt1', 'buyers'])
    execution = Execution(_account_config, source, destination)

    batch = Batch(execution, [{
        'mobileDeviceIds': 'mobileId1'  
        }, {
        'mobileDeviceIds': [
            'mobileId2',
            'mobileId3',
        ]}, {
        'mobileDeviceIds': 'mobileId4'  
        },
    ])

    uploader.process(batch)
    uploader.finish_bundle()

    uploader._get_dv_audience_service.return_value.list.assert_called_once_with(
        advertiserId='advertiser_id',
        pageSize=1,
        filter='displayName : "list_name"'
    )

    test_create_resquest = {
        'displayName': 'list_name',
        'firstAndThirdPartyAudienceType': 'FIRST_AND_THIRD_PARTY_AUDIENCE_TYPE_FIRST_PARTY',
        'audienceType': 'CUSTOMER_MATCH_DEVICE_ID',
        'membershipDurationDays': 10000,
        'description': 'List created automatically by Megalista',
        'appId': 1234,
        'mobileDeviceIdList': {
            'mobileDeviceIds': [       
                'mobileId1',
                'mobileId2',
                'mobileId3', 
                'mobileId4',          
            ]
        }

    }
    uploader._get_dv_audience_service.return_value.create.assert_called_once_with(
        advertiserId='advertiser_id',
        body=test_create_resquest
    )

    assert not error_notifier.were_errors_sent

def test_upload_update_users(mocker, uploader, error_notifier):

    mocker.patch.object(uploader, '_get_dv_audience_service')
    
    audience = MagicMock() 
    audience.firstAndThirdPartyAudienceId = 12345
    audience.displayName = 'list_name'

    audience_list = MagicMockDict()
    audience_list['firstAndThirdPartyAudiences'] = [audience]

    uploader._get_dv_audience_service.return_value.list.return_value.execute.return_value = audience_list
    uploader._get_dv_audience_service.return_value.editCustomerMatchMembers.return_value = MagicMock()

    destination = Destination(
        'dest1',
        DestinationType.DV_CUSTOMER_MATCH_DEVICE_ID_UPLOAD,
        ['advertiser_id', 'list_name', True, 1234]
    )

    source = Source('orig1', SourceType.BIG_QUERY, ['dt1', 'buyers'])
    execution = Execution(_account_config, source, destination)

    batch = Batch(execution, [{
        'mobileDeviceIds': 'mobileId1'  
        }, {
        'mobileDeviceIds': [
            'mobileId2',
            'mobileId3',
        ]}, {
        'mobileDeviceIds': 'mobileId4'  
        },
    ])

    uploader.process(batch)
    uploader.finish_bundle()

    uploader._get_dv_audience_service.return_value.list.assert_called_once_with(
        advertiserId='advertiser_id',
        pageSize=1,
        filter='displayName : "list_name"'
    )

    test_update_resquest = {
        'advertiserId': 'advertiser_id',
        'addedMobileDeviceIdList': {
            'mobileDeviceIds': [       
                'mobileId1',
                'mobileId2',
                'mobileId3', 
                'mobileId4',          
            ]
        }
    }

    uploader._get_dv_audience_service.return_value.editCustomerMatchMembers.assert_any_call(
        firstAndThirdPartyAudienceId=audience_list['firstAndThirdPartyAudiences'][0]['firstAndThirdPartyAudienceId'],
        body=test_update_resquest
    )

    assert not error_notifier.were_errors_sent
