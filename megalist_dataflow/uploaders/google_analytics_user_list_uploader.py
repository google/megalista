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


import logging
import apache_beam as beam
from googleapiclient.discovery import build
from googleapiclient.http import MediaInMemoryUpload
from google.oauth2.credentials import Credentials

from uploaders import google_ads_utils as ads_utils
from uploaders import utils as utils
from utils.execution import DestinationType


class GoogleAnalyticsUserListUploaderDoFn(beam.DoFn):
  def __init__(self,
               oauth_credentials):
    super().__init__()
    self.oauth_credentials = oauth_credentials
    self.active = True

  def _get_analytics_service(self):
    credentials = Credentials(
      token=self.oauth_credentials.get_access_token(),
      refresh_token=self.oauth_credentials.get_refresh_token(),
      client_id=self.oauth_credentials.get_client_id(),
      client_secret=self.oauth_credentials.get_client_secret(),
      token_uri='https://accounts.google.com/o/oauth2/token',
      scopes=["https://www.googleapis.com/auth/analytics.edit", 'https://www.googleapis.com/auth/adwords'])

    service = build('analytics', 'v3', credentials=credentials)
    return service

  def _create_list_if_doesnt_exist(self, analytics, web_property_id, view_ids, list_name, list_definition, ga_account_id, ads_customer_id):
    lists = analytics.management().remarketingAudience().list(
      accountId=ga_account_id, webPropertyId=web_property_id).execute()['items']
    results = list(
      filter(lambda x: x['name'] == list_name, lists))
    if len(results) == 0:
      logging.getLogger().info('%s list does not exist, creating...' % list_name)

      response = analytics.management().remarketingAudience().insert(
        accountId=ga_account_id,
        webPropertyId=web_property_id,
        body={
          'name': list_name,
          'linkedViews': view_ids,
          'linkedAdAccounts': [{
            'type': 'ADWORDS_LINKS',
            'linkedAccountId': ads_customer_id
          }],
          **list_definition
        }).execute()
      id = response['id']
      logging.getLogger().info('%s created with id: %s' % (list_name, id))
    else:
      id = results[0]['id']
      logging.getLogger().info('%s found with id: %s' % (list_name, id))
    return id

  def start_bundle(self):
    pass

  def _create_list(self, web_property_id, view_id, user_id_list_name, buyer_custom_dim, ga_account_id, ads_customer_id):
    analytics = self._get_analytics_service()
    view_ids = [view_id]
    self._create_list_if_doesnt_exist(analytics, web_property_id, view_ids, user_id_list_name, {
      'audienceType': 'SIMPLE',
      'audienceDefinition': {
        'includeConditions': {
          'kind': 'analytics#includeConditions',
          'isSmartList': False,
          'segment': 'users::condition::%s==buyer' % buyer_custom_dim,
          'membershipDurationDays': 365
        }
      }
    }, ga_account_id, ads_customer_id)

  @staticmethod
  def _assert_all_list_names_are_present(any_execution):
    destination = any_execution.destination.destination_metadata
    if len(destination) is not 6:
      raise ValueError('Missing destination information. Found {}'.format(len(destination)))

    if not destination[0] \
        or not destination[1] \
        or not destination[2] \
        or not destination[3] \
        or not destination[4] \
        or not destination[5]:
      raise ValueError('Missing destination information. Received {}'.format(str(destination)))


  @utils.safe_process(logger=logging.getLogger("megalista.GoogleAnalyticsUserListUploader"))
  def process(self, elements, **kwargs):

    if not self.active:
      logging.getLogger().warning('Skipping upload to FA, parameters not configured.')
      return

    ads_utils.assert_elements_have_same_execution(elements)
    any_execution = elements[0]['execution']
    ads_utils.assert_right_type_action(any_execution, DestinationType.GA_USER_LIST_UPLOAD)
    self._assert_all_list_names_are_present(any_execution)

    ads_customer_id = any_execution.account_config.google_ads_account_id
    ga_account_id = any_execution.account_config.google_analytics_account_id

    web_property_id = any_execution.destination.destination_metadata[0]
    view_id = any_execution.destination.destination_metadata[1]
    data_import_name = any_execution.destination.destination_metadata[2]
    user_id_list_name = any_execution.destination.destination_metadata[3]
    user_id_custom_dim = any_execution.destination.destination_metadata[4]
    buyer_custom_dim = any_execution.destination.destination_metadata[5]

    self._do_upload_data(web_property_id, view_id, data_import_name, user_id_list_name, user_id_custom_dim,
                         buyer_custom_dim, ga_account_id, ads_customer_id, utils.extract_rows(elements))

  def _do_upload_data(self, web_property_id, view_id, data_import_name, user_id_list_name, user_id_custom_dim,
                      buyer_custom_dim, ga_account_id, ads_customer_id, rows):
    self._create_list(web_property_id, view_id, user_id_list_name, buyer_custom_dim, ga_account_id, ads_customer_id)

    analytics = self._get_analytics_service()
    data_sources = analytics.management().customDataSources().list(
      accountId=ga_account_id, webPropertyId=web_property_id).execute()['items']
    results = list(
      filter(lambda x: x['name'] == data_import_name, data_sources))
    if len(results) == 1:
      id = results[0]['id']
      logging.getLogger().info("Adding data to %s - %s" % (data_import_name, id))
      body = '\n'.join(['%s,%s' % (user_id_custom_dim, buyer_custom_dim), *['%s,buyer' %
                                                                            row['user_id'] for row in rows]])
      try:
        media = MediaInMemoryUpload(bytes(body, 'UTF-8'),
                                    mimetype='application/octet-stream',
                                    resumable=True)
        analytics.management().uploads().uploadData(
          accountId=ga_account_id,
          webPropertyId=web_property_id,
          customDataSourceId=id,
          media_body=media).execute()
      except Exception as e:
        logging.getLogger().error('Error while uploading GA Data: %s' % e)
    else:
      logging.getLogger().error(
        "%s - data import not found, please configure it in Google Analytics" % data_import_name)
