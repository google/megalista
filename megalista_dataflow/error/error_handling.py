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
import base64
from email.mime.text import MIMEText
from typing import Iterable

from apache_beam.options.value_provider import ValueProvider
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

from models.execution import DestinationType
from models.oauth_credentials import OAuthCredentials


class Error:
  """
    Holds errors executions and respective error messages
  """

  def __init__(self, execution, error_message):
    self._execution = execution
    self._error_message = error_message

  @property
  def execution(self):
    return self._execution

  @property
  def error_message(self):
    return self._error_message


class ErrorNotifier:
  """
    Abstract class to notify errors. The mean is defined by the implementation.
  """

  def notify(self, errors: Iterable[Error]):
    raise NotImplementedError()


class GmailNotifier(ErrorNotifier):
  """
    Notify errors sending emails through the gMail API. Uses the main application credentials.
  """

  def __init__(self, oauth_credentials: OAuthCredentials, email_destinations: ValueProvider):
    self._oauth_credentials = oauth_credentials
    self._email_destinations = email_destinations
    self._parsed_emails = None

  def _get_gmail_service(self):
    credentials = Credentials(
      token=self._oauth_credentials.get_access_token(),
      refresh_token=self._oauth_credentials.get_refresh_token(),
      client_id=self._oauth_credentials.get_client_id(),
      client_secret=self._oauth_credentials.get_client_secret(),
      token_uri='https://accounts.google.com/o/oauth2/token',
      scopes=[
        'https://www.googleapis.com/auth/gmail.send'])

    return build('gmail', 'v1', credentials=credentials)

  @property
  def email_destinations(self) -> Iterable[str]:
    if self._parsed_emails:
      return self._parsed_emails

    self._parsed_emails = list(map(lambda email: email.strip(), self._email_destinations.get().split(',')))
    return self._parsed_emails

  def notify(self, errors: Iterable[Error]):
    # TODO: Send emails
    pass

  def send_test_email(self):
    gmail_service = self._get_gmail_service()

    message = MIMEText('Email test')
    message['to'] = 'antoniomoreira@google.com'
    message['from'] = 'me'
    message['subject'] = '[TEST] Megalista error detected'
    raw = base64.urlsafe_b64encode(message.as_bytes()).decode()

    gmail_service.users().messages().send(userId='me', body={'raw': raw}).execute()


class ErrorHandler:
  """
    Accumulate errors and notify them.
    Only record one message by Execution.
  """

  def __init__(self, destination_type: DestinationType, error_notifier: ErrorNotifier):
    self._destination_type = destination_type
    self._error_notifier = error_notifier
    self._errors = {}

  def add_error(self, error: Error):
    """
      Add an error to be logged.
      Only record one error per Execution, so the output message isn't too long.
    """

    if error.execution.destination.destination_type != self._destination_type:
      raise ValueError(
        f'Received a error of destination type: {error.execution.destination.destination_type}'
        f' but this error handler is initialized with {self._destination_type} destination type')

    self._errors[error.execution] = error

  @property
  def errors(self):
    return self._errors.copy()

  def notify_errors(self):
    """
      Send the errors accumulated by email.
    """
    self._error_notifier.notify(self._errors.values())
