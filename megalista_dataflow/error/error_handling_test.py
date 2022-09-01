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

import pytest
from apache_beam.options.value_provider import StaticValueProvider

from error.error_handling import ErrorHandler, Error, GmailNotifier, ErrorNotifier
from models.execution import DestinationType, Execution, AccountConfig, Source, SourceType, Destination
from models.oauth_credentials import OAuthCredentials

FIRST_EMAIL = 'a@a.com'
SECOND_EMAIL = 'b@b.com'
THIRD_EMAIL = 'c@c.com'
ERROR_MESSAGE = 'error message'
ERROR_FIRST_EXECUTION_FIRST_INPUT = 'Error for first execution, first input'
ERROR_FIRST_EXECUTION_SECOND_INPUT = 'Error for first execution, second input'
ERROR_SECOND_EXECUTION_FIRST_INPUT = 'Error for second execution, first input'


class MockErrorNotifier(ErrorNotifier):
  def __init__(self):
    self.were_errors_sent = False
    self.errors_sent = {}
    self.destination_type = None

  def notify(self, destination_type: DestinationType, errors: Iterable[Error]):
    self.were_errors_sent = True
    self.errors_sent = {error.execution: error.error_message for error in errors}
    self.destination_type = destination_type


# ErrorHandler tests

def create_execution(source_name, destination_name):
  account_config = AccountConfig('', False, '', '', '')
  source = Source(source_name, SourceType.BIG_QUERY, ['', ''])
  destination = Destination(destination_name, DestinationType.ADS_OFFLINE_CONVERSION, [''])
  return Execution(account_config, source, destination)


def test_single_error_per_execution():
  error_handler = ErrorHandler(DestinationType.ADS_OFFLINE_CONVERSION, MockErrorNotifier())

  first_execution = create_execution('source1', 'destination1')
  second_execution = create_execution('source1', 'destination2')

  error_handler.add_error(first_execution, ERROR_FIRST_EXECUTION_FIRST_INPUT)
  error_handler.add_error(first_execution, ERROR_FIRST_EXECUTION_SECOND_INPUT)
  error_handler.add_error(second_execution, ERROR_SECOND_EXECUTION_FIRST_INPUT)

  returned_errors = error_handler.errors
  assert len(returned_errors) == 2
  assert returned_errors.keys() == {first_execution, second_execution}


def test_destination_type_consistency():
  error_handler = ErrorHandler(DestinationType.CM_OFFLINE_CONVERSION, MockErrorNotifier())
  wrong_destination_type_execution = create_execution('source', 'destination')

  with pytest.raises(ValueError):
    error_handler.add_error(wrong_destination_type_execution, ERROR_MESSAGE)


def test_errors_sent_to_error_notifier():
  mock_notifier = MockErrorNotifier()
  error_handler = ErrorHandler(DestinationType.ADS_OFFLINE_CONVERSION, mock_notifier)

  first_execution = create_execution('source1', 'destination1')
  second_execution = create_execution('source1', 'destination2')

  error_handler.add_error(first_execution, ERROR_FIRST_EXECUTION_FIRST_INPUT)
  error_handler.add_error(second_execution, ERROR_SECOND_EXECUTION_FIRST_INPUT)

  error_handler.notify_errors()

  assert mock_notifier.were_errors_sent is True
  assert mock_notifier.errors_sent == {first_execution: ERROR_FIRST_EXECUTION_FIRST_INPUT,
                                       second_execution: ERROR_SECOND_EXECUTION_FIRST_INPUT}
  assert mock_notifier.destination_type == DestinationType.ADS_OFFLINE_CONVERSION


def test_should_not_notify_when_empty_errors():
  mock_notifier = MockErrorNotifier()
  error_handler = ErrorHandler(DestinationType.ADS_OFFLINE_CONVERSION, mock_notifier)

  error_handler.notify_errors()

  assert mock_notifier.were_errors_sent is False


# GmailNotifier tests

def test_multiple_destinations_separated_by_comma():
  credentials = OAuthCredentials('', '', '', '')
  gmail_notifier = GmailNotifier(StaticValueProvider(str, 'true'), credentials,
                                 StaticValueProvider(str, f'{FIRST_EMAIL}, {SECOND_EMAIL} ,{THIRD_EMAIL}'))

  emails = set(gmail_notifier.email_destinations)
  assert len(emails) == 3
  assert set(emails) == {FIRST_EMAIL, THIRD_EMAIL, SECOND_EMAIL}


def test_should_not_notify_when_param_is_false():
  credentials = OAuthCredentials('', '', '', '')
  gmail_notifier = GmailNotifier(StaticValueProvider(str, 'false'), credentials,
                                 StaticValueProvider(str, f'{FIRST_EMAIL}, {SECOND_EMAIL} ,{THIRD_EMAIL}'))

  gmail_notifier.notify(DestinationType.ADS_OFFLINE_CONVERSION, [Error(create_execution('s', 'd'), ERROR_MESSAGE)])


def test_should_not_notify_when_param_is_empty():
  credentials = OAuthCredentials('', '', '', '')
  gmail_notifier = GmailNotifier(StaticValueProvider(str, None), credentials,
                                 StaticValueProvider(str, f'{FIRST_EMAIL}, {SECOND_EMAIL} ,{THIRD_EMAIL}'))

  gmail_notifier.notify(DestinationType.ADS_OFFLINE_CONVERSION, [Error(create_execution('s', 'd'), ERROR_MESSAGE)])
