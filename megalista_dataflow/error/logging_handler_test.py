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
from typing import Iterable
from error.logging_handler import LoggingHandler
import logging

import pytest
from apache_beam.options.value_provider import StaticValueProvider

from error.error_handling import ErrorHandler, Error, GmailNotifier, ErrorNotifier
from models.execution import DestinationType, Execution, AccountConfig, Source, SourceType, Destination
from models.oauth_credentials import OAuthCredentials

def get_log_record_info():
  return logging.LogRecord("unit_test", logging.INFO, '', 1, 'Message Info', None, None)

def get_log_record_error():
  return logging.LogRecord("unit_test", logging.ERROR, '', 1, 'Message Error', None, None)

# ErrorHandler tests
def test_has_errors():
  handler = LoggingHandler()
  assert handler.has_errors == False
  handler.emit(get_log_record_info())
  assert handler.has_errors == False
  handler.emit(get_log_record_error())
  assert handler.has_errors == True

