# Copyright 2020 Google LLC
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

from apache_beam.options.value_provider import StaticValueProvider
from megalist_dataflow.uploaders.campaign_manager_conversion_uploader import CampaignManagerConversionUploaderDoFn
from megalist_dataflow.utils.execution import AccountConfig
from megalist_dataflow.utils.execution import Destination
from megalist_dataflow.utils.execution import DestinationType
from megalist_dataflow.utils.execution import Execution
from megalist_dataflow.utils.execution import Source
from megalist_dataflow.utils.execution import SourceType
from megalist_dataflow.utils.oauth_credentials import OAuthCredentials
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


def test_not_active(mocker, caplog):
  secret = StaticValueProvider(str, 'secret')
  access = StaticValueProvider(str, 'access')
  refresh = StaticValueProvider(str, 'refresh')
  credentials = OAuthCredentials(id, secret, access, refresh)
  uploader_dofn = CampaignManagerConversionUploaderDoFn(None)
  mocker.patch.object(uploader_dofn, '_get_dcm_service')
  uploader_dofn.process([{}],)
  uploader_dofn._get_dcm_service.assert_not_called()
  assert (
      'Skipping upload to Campaign Manager, parameters not configured.'
      in caplog.text)


def test_work_with_empty_elements(uploader, mocker, caplog):
  mocker.patch.object(uploader, '_get_dcm_service')
  uploader.process([],)
  uploader._get_dcm_service.assert_not_called()
  assert 'Skipping upload, received no elements.' in caplog.text


def test_fail_having_more_than_one_execution(uploader):

  source1 = Source('orig1', SourceType.BIG_QUERY, ('dt1', 'buyers'))
  destination1 = Destination(
      'dest1', DestinationType.CM_OFFLINE_CONVERSION, ('a'))

  source2 = Source('orig2', SourceType.BIG_QUERY, ('dt2', 'buyers2'))
  destination2 = Destination(
      'dest2', DestinationType.CM_OFFLINE_CONVERSION, ('a'))

  exec1 = Execution(_account_config, source1, destination1)
  exec2 = Execution(_account_config, source2, destination2)

  with pytest.raises(
      ValueError, match='At least two Execution in a single call'):
    uploader.process([{'execution': exec1}, {'execution': exec2}],)


def test_fail_with_wrong_destination_type(uploader):
  source = Source('orig1', SourceType.BIG_QUERY, ('dt1', 'buyers'))
  destination = Destination('dest1', DestinationType.GA_USER_LIST_UPLOAD, ('a'))
  execution = Execution(_account_config, source, destination)

  with pytest.raises(ValueError, match='Wrong DestinationType received'):
    uploader.process([{'execution': execution}],)


def test_fail_missing_destination_metadata(uploader):
  source = Source('orig1', SourceType.BIG_QUERY, ('dt1', 'buyers'))
  destination = Destination('dest1', DestinationType.CM_OFFLINE_CONVERSION, ())
  execution = Execution(_account_config, source, destination)
  with pytest.raises(ValueError, match='Missing destination information'):
    uploader.process([{'execution': execution}],)

  assert_empty_destination_metadata(uploader, (''))


def assert_empty_destination_metadata(uploader, destination_metadata):
  source = Source('orig1', SourceType.BIG_QUERY, ('dt1', 'buyers'))
  destination = Destination(
      'dest1', DestinationType.CM_OFFLINE_CONVERSION, destination_metadata)
  execution = Execution(_account_config, source, destination)
  with pytest.raises(ValueError, match='Missing destination information'):
    uploader.process([{'execution': execution}],)


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

  uploader._do_process([{
      'execution': execution,
      'row': {
          'gclid': '123'
      }
  }, {
      'execution': execution,
      'row': {
          'gclid': '456'
      }
  }], current_time)

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
      'encryptionInfo': 'AD_SERVING'
  }

  uploader._get_dcm_service().conversions().batchinsert.assert_any_call(
      profileId='dcm_profile_id', body=expected_body)


def test_conversion_upload_match_id(mocker, uploader):
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

  mocker.patch.object(time, 'time')
  time.time.return_value = current_time

  uploader.process([{'execution': execution, 'row': {'matchId': 'abc'}}])

  expected_body = {
      'conversions': [{
          'matchId': 'abc',
          'floodlightActivityId': floodlight_activity_id,
          'floodlightConfigurationId': floodlight_configuration_id,
          'ordinal': math.floor(current_time * 10e5),
          'timestampMicros': math.floor(current_time * 10e5)
      }],
      'encryptionInfo': 'AD_SERVING'
  }
  
  uploader._get_dcm_service().conversions().batchinsert.assert_any_call(
      profileId='dcm_profile_id', body=expected_body)


def test_error_on_api_call(mocker, uploader, caplog):
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

  uploader.process([{'execution': execution, 'row': {'gclid': '123'}}])

  assert 'Error(s) inserting conversions:' in caplog.text
  assert '\t[123]: error_returned' in caplog.text
