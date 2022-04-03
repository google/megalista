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


class MockErrorNotifier(ErrorNotifier):
  def __init__(self):
    self.were_errors_sent = False
    self.errors_sent = {}

  def notify(self, errors: dict[Error]):
    self.were_errors_sent = True
    self.errors_sent = errors


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

  error_handler.add_error(Error(first_execution, 'Error for first execution, fist input'))
  error_handler.add_error(Error(first_execution, 'Error for first execution, second input'))
  error_handler.add_error(Error(second_execution, 'Error for second execution, fist input'))

  returned_errors = error_handler.errors
  assert len(returned_errors) is 2
  assert returned_errors.keys() == {first_execution, second_execution}


def test_destination_type_consistency():
  error_handler = ErrorHandler(DestinationType.CM_OFFLINE_CONVERSION, MockErrorNotifier())
  wrong_destination_type_execution = create_execution('source', 'destination')

  with pytest.raises(ValueError):
    error_handler.add_error(Error(wrong_destination_type_execution, 'error message'))


def test_errors_sent_to_error_notifier():
  mock_notifier = MockErrorNotifier()
  error_handler = ErrorHandler(DestinationType.ADS_OFFLINE_CONVERSION, mock_notifier)

  first_execution = create_execution('source1', 'destination1')
  second_execution = create_execution('source1', 'destination2')

  first_error = Error(first_execution, 'Error for first execution, fist input')
  second_error = Error(second_execution, 'Error for second execution, fist input')
  error_handler.add_error(first_error)
  error_handler.add_error(second_error)

  error_handler.notify_errors()

  assert mock_notifier.were_errors_sent is True
  assert set(mock_notifier.errors_sent) == {first_error, second_error}


# GmailNotifier tests

def test_multiple_destinations_separated_by_comma():
  first_email = 'a@a.com'
  second_email = 'b@b.com'
  third_email = 'c@c.com'

  credentials = OAuthCredentials('', '', '', '')
  gmail_notifier = GmailNotifier(credentials, StaticValueProvider(str, f'{first_email}, {second_email} ,{third_email}'))

  emails = set(gmail_notifier.email_destinations)
  assert len(emails) == 3
  assert set(emails) == {first_email, third_email, second_email}
