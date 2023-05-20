# Copyright 2022 Google LLC
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
from models.options import DataflowOptions
from models.json_config import JsonConfig
from sources.json_execution_source import JsonExecutionSource
from models.execution import AccountConfig, DataRow, DataRowsGroupedBySource, SourceType, DestinationType, TransactionalType, Execution, Source, Destination, ExecutionsGroupedBySource
from apache_beam.options.value_provider import StaticValueProvider


from typing import List, Dict
import pytest
from pytest_mock import MockFixture

_JSON = '{"GoogleAdsAccountId":"","GoogleAdsMCC":false,"AppId":"","GoogleAnalyticsAccountId":"","CampaignManagerAccountId":"","Sources":[{"Name":"[BQ] Contact Info","Type":"BIG_QUERY","Dataset":"megalista_data","Table":"customer_match_contact_info"},{"Name":"[BQ] Contact Info - Email","Type":"BIG_QUERY","Dataset":"megalista_data","Table":"customer_match_contact_info_email"},{"Name":"[BQ] Contact Info - Phone","Type":"BIG_QUERY","Dataset":"megalista_data","Table":"customer_match_contact_info_phone"},{"Name":"[BQ] Contact Info - Mailing Address","Type":"BIG_QUERY","Dataset":"megalista_data","Table":"customer_match_contact_info_mailing_address"},{"Name":"[BQ] Ads - Offline Conversion (click)","Type":"BIG_QUERY","Dataset":"megalista_data","Table":"ads_offline_conversion"},{"Name":"[BQ - Carga] Contact Info 10m","Type":"BIG_QUERY","Dataset":"megalista_data","Table":"customer_match_contact_info_teste_carga_1"}],"Destinations":[{"Name":"[BQ] Contact Info","Type":"ADS_CUSTOMER_MATCH_CONTACT_INFO_UPLOAD","Metadata":["Megalista - Testing - Contact Info","ADD","10"]},{"Name":"[BQ] Contact Info - Email","Type":"ADS_CUSTOMER_MATCH_CONTACT_INFO_UPLOAD","Metadata":["Megalista - Testing - Contact Info (email)","ADD","10"]},{"Name":"[BQ] Contact Info - Phone","Type":"ADS_CUSTOMER_MATCH_CONTACT_INFO_UPLOAD","Metadata":["Megalista - Testing - Contact Info (phone)","ADD","10"]},{"Name":"[BQ] Contact Info - Mailing Address","Type":"ADS_CUSTOMER_MATCH_CONTACT_INFO_UPLOAD","Metadata":["Megalista - Testing - Contact Info (mailing address)","ADD","10"]},{"Name":"[BQ] Ads - Offline Conversion (click)","Type":"ADS_OFFLINE_CONVERSION","Metadata":["Megalista - testing - Offline Conversions (click)"]},{"Name":"[BQ] DV360 Contact Info","Type":"DV_CUSTOMER_MATCH_CONTACT_INFO_UPLOAD","Metadata":["633801967","Megalista - Testing - DV360 - Contact Info"]}],"Connections":[{"Enabled":false,"Source":"[BQ] Contact Info","Destination":"[BQ] Contact Info"},{"Enabled":false,"Source":"[BQ] Contact Info - Email","Destination":"[BQ] Contact Info - Email"},{"Enabled":false,"Source":"[BQ] Contact Info - Phone","Destination":"[BQ] Contact Info - Phone"},{"Enabled":false,"Source":"[BQ] Contact Info - Mailing Address","Destination":"[BQ] Contact Info - Mailing Address"},{"Enabled":false,"Source":"[BQ] Ads - Offline Conversion (click)","Destination":"[BQ] Ads - Offline Conversion (click)"},{"Enabled":false,"Source":"[BQ] Contact Info","Destination":"[BQ] DV360 Contact Info"}]}'

@pytest.fixture
def json_config(mocker: MockFixture):
    json_config = JsonConfig(DataflowOptions())
    return json_config

def patch_json_config(mocker, json_config):
    mocker.patch.object(json_config, '_get_json')
    json_config._get_json.return_value = _JSON


def test_read_destinations(mocker, json_config):
    patch_json_config(mocker, json_config)
    json_data = json_config.parse_json_from_url('')

    destinations: Dict[str, Destination] = JsonExecutionSource._read_destination(json_config, json_data)

    key = '[BQ] Contact Info'
    assert len(destinations) == 6
    assert destinations[key].destination_name == key
    assert destinations[key].destination_type == DestinationType.ADS_CUSTOMER_MATCH_CONTACT_INFO_UPLOAD
    assert destinations[key].destination_metadata == ['Megalista - Testing - Contact Info', 'ADD', '10']

def test_read_sources(mocker, json_config):
    patch_json_config(mocker, json_config)

    json_data = json_config.parse_json_from_url('')

    sources: Dict[str, Source] = JsonExecutionSource._read_sources(json_config, json_data)

    key = '[BQ] Contact Info'
    assert len(sources) == 6
    assert sources[key].source_name == key
    assert sources[key].source_type == SourceType.BIG_QUERY
    assert sources[key].source_metadata == ['megalista_data', 'customer_match_contact_info']

def test_count(mocker, json_config):
    patch_json_config(mocker, json_config)

    execution_source: JsonExecutionSource = JsonExecutionSource(json_config, StaticValueProvider(str, ''))

    assert execution_source.count() == 6