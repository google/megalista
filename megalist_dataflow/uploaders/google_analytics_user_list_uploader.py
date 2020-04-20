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

from uploaders import utils as utils
from utils.execution import Action


class GoogleAnalyticsUserListUploaderDoFn(beam.DoFn):
  def __init__(self,
               oauth_credentials,
               account_id,
               google_ads_account):
    super().__init__()
    self.oauth_credentials = oauth_credentials
    self.account_id = account_id
    self.google_ads_account = google_ads_account
    self.active = True
    if self.account_id is None or self.google_ads_account is None:
      self.active = False

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

  def _create_list_if_doesnt_exist(self, analytics, web_property_id, view_ids, list_name, list_definition):
    lists = analytics.management().remarketingAudience().list(
      accountId=self.account_id.get(), webPropertyId=web_property_id).execute()['items']
    results = list(
      filter(lambda x: x['name'] == list_name, lists))
    if len(results) == 0:
      logging.getLogger().info('%s list does not exist, creating...' % list_name)

      print('------------------------')
      print(str({'name': list_name,
                 'linkedViews': view_ids,
                 'linkedAdAccounts': [{
                   'type': 'ADWORDS_LINKS',
                   'linkedAccountId': self.google_ads_account.get()
                 }],
                 **list_definition}))

      response = analytics.management().remarketingAudience().insert(
        accountId=self.account_id.get(),
        webPropertyId=web_property_id,
        body={
          'name': list_name,
          'linkedViews': view_ids,
          'linkedAdAccounts': [{
            'type': 'ADWORDS_LINKS',
            'linkedAccountId': self.google_ads_account.get()
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

  def _create_list(self, web_property_id, view_id, user_id_list_name, buyer_custom_dim):
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
    })

  def process(self, elements, **kwargs):

    # print('--------------- ' + str(elements[0]['execution'].destination_metadata))

    if not self.active:
      logging.getLogger().warning('Skipping upload to FA, parameters not configured.')
      return

    if len(elements) == 0:
      logging.getLogger().warning('Skipping upload to GA, received no elements.')
      return

    utils.assert_elements_have_same_execution(elements)
    any_execution = elements[0]['execution']
    utils.assert_right_type_action(any_execution, Action.GA_USER_LIST_UPLOAD)

    web_property_id = any_execution.destination_metadata[0]
    view_id = any_execution.destination_metadata[1]
    data_import_name = any_execution.destination_metadata[2]
    user_id_list_name = any_execution.destination_metadata[3]
    user_id_custom_dim = any_execution.destination_metadata[4]
    buyer_custom_dim = any_execution.destination_metadata[5]

    self._do_upload_data(web_property_id, view_id, data_import_name, user_id_list_name, user_id_custom_dim,
                         buyer_custom_dim, utils.extract_rows(elements))

  def _do_upload_data(self, web_property_id, view_id, data_import_name, user_id_list_name, user_id_custom_dim,
                      buyer_custom_dim, rows):
    self._create_list(web_property_id, view_id, user_id_list_name, buyer_custom_dim)

    analytics = self._get_analytics_service()
    data_sources = analytics.management().customDataSources().list(
      accountId=self.account_id.get(), webPropertyId=web_property_id).execute()['items']
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
          accountId=self.account_id.get(),
          webPropertyId=web_property_id,
          customDataSourceId=id,
          media_body=media).execute()
      except Exception as e:
        logging.getLogger().error('Error while uploading GA Data: %s' % e)
    else:
      logging.getLogger().error(
        "%s - data import not found, please configure it in Google Analytics" % data_import_name)
