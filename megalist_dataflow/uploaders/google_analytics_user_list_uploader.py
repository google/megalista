# Copyright 2019 Google LLC

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import apache_beam as beam
from apiclient.discovery import build
from apiclient.http import MediaInMemoryUpload
from google.oauth2.credentials import Credentials


class GoogleAnalyticsUserListUploaderDoFn(beam.DoFn):
    def __init__(self, oauth_credentials, accountId, webPropertyId, google_ads_account, user_id_custom_dim, buyer_custom_dim):
        self.oauth_credentials = oauth_credentials
        self.accountId = accountId
        self.webPropertyId = webPropertyId
        self.google_ads_account = google_ads_account
        self.user_id_custom_dim = user_id_custom_dim
        self.buyer_custom_dim = buyer_custom_dim
        self.list_name = 'Megalist - GA - Buyers'
        self.rev_list_name = 'Megalist - GA - Potential New Buyers'
        self.data_source_import_name = 'Megalist - Import'
        self.active = True
        if self.accountId is None or self.webPropertyId is None or self.google_ads_account is None:
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

    def _create_list_if_doesnt_exist(self, analytics, view_ids, list_name, list_definition):
        lists = analytics.management().remarketingAudience().list(
            accountId=self.accountId.get(), webPropertyId=self.webPropertyId.get()).execute()['items']
        results = list(
            filter(lambda x: x['name'] == list_name, lists))
        if len(results) == 0:
            logging.getLogger().info('%s list does not exist, creating...' % list_name)
            response = analytics.management().remarketingAudience().insert(
                accountId=self.accountId,
                webPropertyId=self.webPropertyId,
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
        if self.active == False:
            logging.getLogger().warn(
                "Skipping upload to Google Analytics, parameters not configured.")
            return
        analytics = self._get_analytics_service()
        list_profiles = analytics.management().profiles().list(
            accountId=self.accountId.get(), webPropertyId=self.webPropertyId.get()).execute()
        view_ids = [item['id'] for item in list_profiles['items']]
        self._create_list_if_doesnt_exist(analytics, view_ids, self.list_name, {
            'audienceType': 'SIMPLE',
            'audienceDefinition': {
                'includeConditions': {
                    'kind': 'analytics#includeConditions',
                    'isSmartList': False,
                    'segment': 'users::condition::ga:transactions>0,ga:goalCompletionsAll>0',
                    'membershipDurationDays': 365
                }
            }
        })
        self._create_list_if_doesnt_exist(analytics, view_ids, self.rev_list_name, {
            'audienceType': 'STATE_BASED',
            'stateBasedAudienceDefinition': {
                'includeConditions': {
                    'daysToLookBack': 30,
                    'segment': '',
                    'membershipDurationDays': 365,
                    'isSmartList': False
                },
                'excludeConditions': {
                    'exclusionDuration': 'TEMPORARY',
                    'segment': 'users::condition::ga:goalCompletionsAll>0,ga:transactions>0'
                }
            }
        })

    def process(self, elements):
        if self.active == False:
            return
        analytics = self._get_analytics_service()
        data_sources = analytics.management().customDataSources().list(
            accountId=self.accountId.get(), webPropertyId=self.webPropertyId.get()).execute()['items']
        results = list(
            filter(lambda x: x['name'] == self.data_source_import_name, data_sources))
        if (len(results) == 1):
            id = results[0]['id']
            logging.getLogger().info("Adding data to %s - %s" %
                                     (self.data_source_import_name, id))
            body = '\n'.join(['%s,%s' % (self.user_id_custom_dim, self.buyer_custom_dim), *['%s,buyer' %
                                                                                            element['user_id'] for element in elements]])
            try:
                media = MediaInMemoryUpload(bytes(body, 'UTF-8'),
                                            mimetype='application/octet-stream',
                                            resumable=True)
                daily_upload = analytics.management().uploads().uploadData(
                    accountId=self.accountId.get(),
                    webPropertyId=self.webPropertyId.get(),
                    customDataSourceId=id,
                    media_body=media).execute()
            except Exception as e:
                logging.getLogger().error('Error while uploading GA Data: %s' % (e))
        else:
            logging.getLogger().error("%s - data import not found, please configure it in Google Analytics" %
                                      self.data_source_import_name)
