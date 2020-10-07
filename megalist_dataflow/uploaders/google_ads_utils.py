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
import datetime
from utils.execution import DestinationType
from utils.execution import Execution
import pytz

timezone = pytz.timezone('America/Sao_Paulo')


def get_ads_service(service_name, version, oauth_credentials, developer_token,
                    customer_id):
  from googleads import adwords
  from googleads import oauth2
  oauth2_client = oauth2.GoogleRefreshTokenClient(
      oauth_credentials.get_client_id(), oauth_credentials.get_client_secret(),
      oauth_credentials.get_refresh_token())
  client = adwords.AdWordsClient(
      developer_token,
      oauth2_client,
      'MegaList Dataflow',
      client_customer_id=customer_id)
  return client.GetService(service_name, version=version)


def assert_elements_have_same_execution(elements):
  if not elements or 'execution' not in elements[0]:
    raise ValueError('No executions found')

  last_execution = elements[0]['execution']
  for element in elements:
    current_execution = element['execution']
    if current_execution != last_execution:
      raise ValueError((f'At least two Execution in a single call '
                        f'({str(current_execution)}) and '
                        f'({str(last_execution)})'))
    last_execution = current_execution


def assert_right_type_action(execution: Execution,
                             expected_action: DestinationType) -> None:
  if execution.destination.destination_type != expected_action:
    raise ValueError((f'Wrong DestinationType received: '
                      f'{execution.destination.destination_type.name} - '
                      f'Expected: {expected_action.name}'))


def format_date(date):
  if isinstance(date, datetime.datetime):
    pdate = date
  else:
    pdate = datetime.datetime.strptime(date, '%Y-%m-%dT%H:%M:%S.%f')

  return f'{datetime.datetime.strftime(pdate, "%Y%m%d %H%M%S")} {timezone.zone}'
