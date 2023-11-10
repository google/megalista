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
from uploaders.google_ads.conversions.google_ads_ssi_uploader import (
    GoogleAdsSSIUploaderDoFn,
)

_ads_account_id = "1234567890"
_ads_mcc_account_id = "0987654321"
_ads_account_override = "12121212121"
_ga_account_id = "3456789"

_account_config = AccountConfig(_ads_account_id, False, _ga_account_id, "", "")
_mcc_account_config = AccountConfig(_ads_mcc_account_id, True, _ga_account_id, "", "")

_conversion_name = "ssi_conversion"
_should_hash = True

_custom_key = None

_time1 = "2020-04-09T14:13:55.0005"
_time1_result = "2020-04-09 14:13:55-03:00"

_time2 = "2020-04-09T13:13:55.0005"
_time2_result = "2020-04-09 13:13:55-03:00"


@pytest.fixture
def uploader(mocker):
    mocker.patch("google.ads.googleads.client.GoogleAdsClient")
    mocker.patch("google.ads.googleads.oauth2")
    id = StaticValueProvider(str, "id")
    secret = StaticValueProvider(str, "secret")
    access = StaticValueProvider(str, "access")
    refresh = StaticValueProvider(str, "refresh")
    credentials = OAuthCredentials(id, secret, access, refresh)
    return GoogleAdsSSIUploaderDoFn(
        credentials,
        StaticValueProvider(str, "devtoken"),
        ErrorHandler(DestinationType.ADS_SSI_UPLOAD, MockErrorNotifier()),
    )


@pytest.fixture
def ssi_batch():
    return create_batch(_account_config, _ads_account_id, _custom_key, "", "")


@pytest.fixture
def ssi_batch_with_account_and_custom_key():
    return create_batch(_account_config, _ads_account_override, "category", "", "")


@pytest.fixture
def ssi_batch_with_mcc_account_override():
    return create_batch(_mcc_account_config, _ads_account_id, _custom_key, "", "")


@pytest.fixture
def ssi_batch_with_consent_override():
    return create_batch(
        _account_config, _ads_account_id, _custom_key, "GRANTED", "GRANTED"
    )


def create_batch(
    account_config, account_override, custom_key, user_data_consent, ad_personalization
):
    source = Source("orig1", SourceType.BIG_QUERY, ("dt1", "buyers"))
    destination = Destination(
        "dest1",
        DestinationType.ADS_SSI_UPLOAD,
        [
            _conversion_name,
            account_override,
            _should_hash,
            custom_key,
            user_data_consent,
            ad_personalization,
        ],
    )
    execution = Execution(account_config, source, destination)

    return Batch(
        execution,
        [
            {
                "hashed_email": "a@a.com",
                "time": _time1,
                "amount": 123,
                "currency_code": "BRL",
                "custom_value": "my_value_1"
            },
            {
                "hashed_email": "b@b.com",
                "time": _time2,
                "amount": 234,
                "currency_code": "BRL",
                 "custom_value": "my_value_2"
            },
        ],
    )


def test_get_service(mocker, uploader):
    assert uploader._get_offline_user_data_job_service(mocker.ANY) is not None


def test_fail_missing_destination_metadata(uploader, mocker):
    mocker.patch.object(uploader, "_get_offline_user_data_job_service")
    source = Source("orig1", SourceType.BIG_QUERY, ("dt1", "buyers"))
    destination = Destination("dest1", DestinationType.ADS_SSI_UPLOAD, ["1", "2"])
    execution = Execution(_account_config, source, destination)
    batch = Batch(execution, [])
    uploader.process(batch)
    uploader._get_offline_user_data_job_service.assert_not_called()


def test_conversion_upload(mocker, uploader, ssi_batch):
    mocker.patch.object(uploader, "_get_offline_user_data_job_service")
    mocker.patch.object(uploader, "_get_resource_name")

    resource_name = (
        uploader._get_offline_user_data_job_service.return_value.create_offline_user_data_job.return_value.resource_name
    )
    conversion_name_resource_name = uploader._get_resource_name.return_value

    uploader.process(ssi_batch)

    data_insertion_payload = {
        "resource_name": resource_name,
        "enable_partial_failure": False,
        "operations": [
            {
                "create": {
                    "user_identifiers": [{"hashed_email": "a@a.com"}],
                    "transaction_attribute": {
                        "conversion_action": conversion_name_resource_name,
                        "currency_code": "BRL",
                        "transaction_amount_micros": 123,
                        "transaction_date_time": _time1_result,
                    },
                }
            },
            {
                "create": {
                    "user_identifiers": [{"hashed_email": "b@b.com"}],
                    "transaction_attribute": {
                        "conversion_action": conversion_name_resource_name,
                        "currency_code": "BRL",
                        "transaction_amount_micros": 234,
                        "transaction_date_time": _time2_result,
                    },
                }
            },
        ],
    }

    uploader._get_offline_user_data_job_service.assert_called_with(_ads_account_id)
    uploader._get_offline_user_data_job_service.return_value.add_offline_user_data_job_operations.assert_any_call(
        request=data_insertion_payload
    )


def test_conversion_upload_account_and_custom_key(
    mocker, uploader, ssi_batch_with_account_and_custom_key
):
    mocker.patch.object(uploader, "_get_offline_user_data_job_service")
    mocker.patch.object(uploader, "_get_resource_name")

    resource_name = (
        uploader._get_offline_user_data_job_service.return_value.create_offline_user_data_job.return_value.resource_name
    )
    conversion_name_resource_name = uploader._get_resource_name.return_value

    uploader.process(ssi_batch_with_account_and_custom_key)

    data_insertion_payload = {
        "resource_name": resource_name,
        "enable_partial_failure": False,
        "operations": [
            {
                "create": {
                    "user_identifiers": [{"hashed_email": "a@a.com"}],
                    "transaction_attribute": {
                        "conversion_action": conversion_name_resource_name,
                        "currency_code": "BRL",
                        "transaction_amount_micros": 123,
                        "transaction_date_time": _time1_result,
                        "custom_value" : "my_value_1",
                    },
                }
            },
            {
                "create": {
                    "user_identifiers": [{"hashed_email": "b@b.com"}],
                    "transaction_attribute": {
                        "conversion_action": conversion_name_resource_name,
                        "currency_code": "BRL",
                        "transaction_amount_micros": 234,
                        "transaction_date_time": _time2_result,
                        "custom_value" : "my_value_2",
                    },
                }
            },
        ],
    }

    uploader._get_offline_user_data_job_service.assert_called_with(
        _ads_account_override
    )
    uploader._get_offline_user_data_job_service.return_value.add_offline_user_data_job_operations.assert_any_call(
        request=data_insertion_payload
    )


def test_conversion_mcc_account_override(
    mocker, uploader, ssi_batch_with_mcc_account_override
):
    mocker.patch.object(uploader, "_get_offline_user_data_job_service")
    mocker.patch.object(uploader, "_get_resource_name")

    resource_name = (
        uploader._get_offline_user_data_job_service.return_value.create_offline_user_data_job.return_value.resource_name
    )
    conversion_name_resource_name = uploader._get_resource_name.return_value

    uploader.process(ssi_batch_with_mcc_account_override)

    data_insertion_payload = {
        "resource_name": resource_name,
        "enable_partial_failure": False,
        "operations": [
            {
                "create": {
                    "user_identifiers": [{"hashed_email": "a@a.com"}],
                    "transaction_attribute": {
                        "conversion_action": conversion_name_resource_name,
                        "currency_code": "BRL",
                        "transaction_amount_micros": 123,
                        "transaction_date_time": _time1_result,
                    },
                }
            },
            {
                "create": {
                    "user_identifiers": [{"hashed_email": "b@b.com"}],
                    "transaction_attribute": {
                        "conversion_action": conversion_name_resource_name,
                        "currency_code": "BRL",
                        "transaction_amount_micros": 234,
                        "transaction_date_time": _time2_result,
                    },
                }
            },
        ],
    }

    uploader._get_offline_user_data_job_service.assert_called_with(_ads_mcc_account_id)
    uploader._get_offline_user_data_job_service.return_value.add_offline_user_data_job_operations.assert_any_call(
        request=data_insertion_payload
    )


def test_conversion_upload_with_consent(
    mocker, uploader, ssi_batch_with_consent_override
):
    mocker.patch.object(uploader, "_get_offline_user_data_job_service")
    mocker.patch.object(uploader, "_get_resource_name")

    resource_name = (
        uploader._get_offline_user_data_job_service.return_value.create_offline_user_data_job.return_value.resource_name
    )
    conversion_name_resource_name = uploader._get_resource_name.return_value

    uploader.process(ssi_batch_with_consent_override)

    data_insertion_payload = {
        "resource_name": resource_name,
        "enable_partial_failure": False,
        "operations": [
            {
                "create": {
                    "user_identifiers": [{"hashed_email": "a@a.com"}],
                    "transaction_attribute": {
                        "conversion_action": conversion_name_resource_name,
                        "currency_code": "BRL",
                        "transaction_amount_micros": 123,
                        "transaction_date_time": _time1_result,
                    },
                    "consent": {
                        "ad_user_data": "GRANTED",
                        "ad_personalization": "GRANTED",
                    },
                }
            },
            {
                "create": {
                    "user_identifiers": [{"hashed_email": "b@b.com"}],
                    "transaction_attribute": {
                        "conversion_action": conversion_name_resource_name,
                        "currency_code": "BRL",
                        "transaction_amount_micros": 234,
                        "transaction_date_time": _time2_result,
                    },
                    "consent": {
                        "ad_user_data": "GRANTED",
                        "ad_personalization": "GRANTED",
                    },
                }
            },
        ],
    }

    uploader._get_offline_user_data_job_service.assert_called_with(_ads_account_id)
    uploader._get_offline_user_data_job_service.return_value.add_offline_user_data_job_operations.assert_any_call(
        request=data_insertion_payload
    )
