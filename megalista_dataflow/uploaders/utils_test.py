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
from config import logging

from error.error_handling import ErrorHandler
from error.error_handling_test import MockErrorNotifier
from models.execution import Batch, DestinationType, Destination, Source, SourceType, Execution, AccountConfig
from uploaders import utils
from uploaders.uploaders import MegalistaUploader

error_message = 'Test error message'


class MockUploader(MegalistaUploader):
  def __init__(self, error_handler: ErrorHandler):
    super().__init__(error_handler)

  @utils.safe_process(logger=logging.get_logger('megalista.UtilsTest'))
  def process(self, batch: Batch, **kwargs):
    self._add_error(batch.execution, error_message)


def test_email_sending_on_safe_process():
  notifier = MockErrorNotifier()
  uploader = MockUploader(ErrorHandler(DestinationType.ADS_OFFLINE_CONVERSION, notifier))

  destination = Destination(
    'dest1', DestinationType.ADS_OFFLINE_CONVERSION, ['user_list'])
  source = Source('orig1', SourceType.BIG_QUERY, ['dt1', 'buyers'])
  execution = Execution(AccountConfig('123-45567-890', False, 'ga_account_id', '', ''), source, destination)
  uploader.process(Batch(execution, [{}]))
  uploader.finish_bundle()

  assert notifier.were_errors_sent
  assert notifier.errors_sent == {execution: error_message}
