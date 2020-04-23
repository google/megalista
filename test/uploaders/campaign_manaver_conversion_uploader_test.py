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

import pytest
import time
from apache_beam.options.value_provider import StaticValueProvider

from uploaders.campaign_manager_conversion_uploader import CampaignManagerConversionUploaderDoFn
from utils.execution import Execution, SourceType, Action
from utils.oauth_credentials import OAuthCredentials


@pytest.fixture
def uploader(mocker):
  id = StaticValueProvider(str, "id")
  secret = StaticValueProvider(str, "secret")
  access = StaticValueProvider(str, "access")
  refresh = StaticValueProvider(str, "refresh")
  credentials = OAuthCredentials(id, secret, access, refresh)
  return CampaignManagerConversionUploaderDoFn(credentials, StaticValueProvider(str, "dcm_profile_id"))


def test_get_service(uploader):
  assert uploader._get_dcm_service() is not None


def test_not_active(mocker, caplog):
  secret = StaticValueProvider(str, "secret")
  access = StaticValueProvider(str, "access")
  refresh = StaticValueProvider(str, "refresh")
  credentials = OAuthCredentials(id, secret, access, refresh)
  uploader = CampaignManagerConversionUploaderDoFn(credentials, None)
  mocker.patch.object(uploader, '_get_dcm_service')
  uploader.process([], )
  uploader._get_dcm_service.assert_not_called()
  assert 'Skipping upload to Campaign Manager, parameters not configured.' in caplog.text


def test_work_with_empty_elements(uploader, mocker, caplog):
  mocker.patch.object(uploader, '_get_dcm_service')
  uploader.process([], )
  uploader._get_dcm_service.assert_not_called()
  assert 'Skipping upload to Campaign Manager, received no elements.' in caplog.text


def test_fail_having_more_than_one_execution(uploader):
  exec1 = Execution('orig1', SourceType.BIG_QUERY, ('dt1', 'buyers'), 'dest1', Action.CM_OFFLINE_CONVERSION, ('a'))
  exec2 = Execution('origi2', SourceType.BIG_QUERY, ('dt2', 'buyers2'), 'dest2', Action.CM_OFFLINE_CONVERSION, ('a'))

  with pytest.raises(ValueError, match='At least two Execution in a single call'):
    uploader.process([{'execution': exec1}, {'execution': exec2}], )


def test_fail_with_wrong_action(uploader):
  execution = Execution('orig1', SourceType.BIG_QUERY, ('dt1', 'buyers'), 'dest1', Action.GA_USER_LIST_UPLOAD, ('a'))

  with pytest.raises(ValueError, match='Wrong Action received'):
    uploader.process([{'execution': execution}], )


def test_fail_missing_destination_metadata(uploader):
  execution = Execution('orig1', SourceType.BIG_QUERY, ('dt1', 'buyers'), 'dest1', Action.CM_OFFLINE_CONVERSION, ())
  with pytest.raises(ValueError, match='Missing destination information'):
    uploader.process([{'execution': execution}], )

  assert_empty_destination_metadata(uploader, (''))


def assert_empty_destination_metadata(uploader, destination_metadata):
  execution = Execution('orig1', SourceType.BIG_QUERY, ('dt1', 'buyers'), 'dest1', Action.CM_OFFLINE_CONVERSION,
                        destination_metadata)
  with pytest.raises(ValueError, match='Missing destination information'):
    uploader.process([{'execution': execution}], )


def test_conversion_upload(mocker, uploader):
  mocker.patch.object(uploader, '_get_dcm_service')

  floodlight_activity_id = 'floodlight_activity_id'
  floodlight_configuration_id = 'floodlight_configuration_id'

  execution = Execution('orig1', SourceType.BIG_QUERY, ('dt1', 'buyers'), 'dest1', Action.CM_OFFLINE_CONVERSION,
                        (floodlight_activity_id, floodlight_configuration_id))

  current_time = time.time()

  uploader._do_process([{'execution': execution, "row": {'gclid': '123'}},
                        {'execution': execution, 'row': {'gclid': '456'}}], current_time)

  uploader._get_dcm_service().conversions().batchinsert.assert_any_call(
    profileId='dcm_profile_id', body={
      'conversions': [
        {
          'gclid': '123',
          'floodlightActivityId': floodlight_activity_id,
          'floodlightConfigurationId': floodlight_configuration_id,
          'ordinal': math.floor(current_time * 10e5),
          'timestampMicros': math.floor(current_time * 10e5)
        },
        {
          'gclid': '456',
          'floodlightActivityId': floodlight_activity_id,
          'floodlightConfigurationId': floodlight_configuration_id,
          'ordinal': math.floor(current_time * 10e5),
          'timestampMicros': math.floor(current_time * 10e5)
        }
      ],
      'encryptionInfo': 'AD_SERVING'
    }
  )


def test_error_on_api_call(mocker, uploader, caplog):
  mocker.patch.object(uploader, '_get_dcm_service')
  service = mocker.MagicMock()
  uploader._get_dcm_service.return_value = service

  service.conversions().batchinsert().execute.return_value = \
    {
      'hasFailures': True,
      'status': [
        {
          'errors': [
            {
              'code': '123',
              'message': 'error_returned'
            }
          ]
        }
      ]
    }

  execution = Execution('orig1', SourceType.BIG_QUERY, ('dt1', 'buyers'), 'dest1', Action.CM_OFFLINE_CONVERSION,
                        ('a', 'b'))
  uploader.process([{'execution': execution, "row": {'gclid': '123'}}])

  assert 'Error(s) inserting conversions:' in caplog.text
  assert '\t[123]: error_returned' in caplog.text
