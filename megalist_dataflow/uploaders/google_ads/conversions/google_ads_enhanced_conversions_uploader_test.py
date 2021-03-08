"""Tests for megalist_dataflow.uploaders.google_ads.google_ads_enhanced_conversions_uploader."""

from uploaders.google_ads.conversions import google_ads_enhanced_conversions_uploader
from models.oauth_credentials import OAuthCredentials
from apache_beam.options.value_provider import StaticValueProvider
from models.execution import Batch, Execution, AccountConfig, Source, Destination, SourceType, DestinationType

import requests_mock
import pytest
import logging


@pytest.fixture
def uploader():
    credential_id = StaticValueProvider(str, 'id')
    secret = StaticValueProvider(str, 'secret')
    access = StaticValueProvider(str, 'access')
    refresh = StaticValueProvider(str, 'refresh')
    credentials = OAuthCredentials(credential_id, secret, access, refresh)
    return google_ads_enhanced_conversions_uploader.GoogleAdsEnhancedConversionsUploaderDoFn(credentials)


def test_request_with_all_info(uploader, mocker):
    mocker.patch.object(uploader, '_get_access_token')
    with requests_mock.Mocker() as m:
        m.post(requests_mock.ANY, status_code=200)
        account_config = AccountConfig(
            "123-456-7890", False, "UA-123", "CM-123", "com.app")
        source = Source('ecs', SourceType.BIG_QUERY, ['megalista', 'ecs'])
        destination = Destination('ecd', DestinationType.ADS_ENHANCED_CONVERSION, [
                                  'label', 'id', 'BRL'])
        execution = Execution(account_config, source, destination)

        element = {
            'gclid': 'Cj0KCQjwj7v0BRDOARIsAGh37ipEIqtU82p7-VKMsS1Gu-jVAtfAAr0-hsWzLv4kiZ9EpLpZpJWhUaArEnEALw_wcB',
            'conversion_time': '1615230348456000',
            'oid': '1',
            'value': '15000000',
            'hashedEmail': 'b83c49e9841ea158035dc4ac2587c512c26e5a3dbc636d68389133b5e8a6c3ca',
            'addressInfo': {
                'hashedFirstName': '318b22d6258b32b559d9abf7b60a8db37937f68e5c5994abb5ba3cb9610405a4',
                'hashedLastName': 'bd442dfc1a89a054a557a516ccec91b1430f13dc9157252e0bc683f75d82cd81',
                'countryCode': 'Brasil',
                'zipCode': '04304032'
            }
        }

        next(uploader.process(Batch(execution, [element])))

        assert m.call_count == 1
        assert m.last_request.query == "gclid=cj0kcqjwj7v0brdoarisagh37ipeiqtu82p7-vkmss1gu-jvatfaar0-hswzlv4kiz9eplpzpjwhuaarenealw_wcb&conversion_time=1615230348456000&conversion_tracking_id=id&label=label&oid=1&value=15000000&currency_code=brl"
        assert m.last_request.json() == {
            'pii_data': {
                'address': [
                    {
                        'hashed_first_name': '318b22d6258b32b559d9abf7b60a8db37937f68e5c5994abb5ba3cb9610405a4',
                        'hashed_last_name': 'bd442dfc1a89a054a557a516ccec91b1430f13dc9157252e0bc683f75d82cd81',
                        'country': 'Brasil', 'postcode': '04304032'
                    }
                ],
                'hashed_email': 'b83c49e9841ea158035dc4ac2587c512c26e5a3dbc636d68389133b5e8a6c3ca'
            },
            'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36'
        }
